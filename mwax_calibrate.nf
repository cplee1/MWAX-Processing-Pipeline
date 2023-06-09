#!/usr/bin/env nextflow

if ( params.flagged_tiles != '' )
    flag_arg = "--tile-flags ${params.flagged_tiles.split(',').join(' ')}"
else
    flag_arg = ''

process check_cal_directory {
    input:
    tuple val(calid), val(cal_dir)

    output:
    tuple val(calid), val(cal_dir), env(METAFITS)

    script:
    """
    set -eux

    if [[ ! -d ${cal_dir} ]]; then
        echo "Error: Cannot locate calibration directory ${cal_dir}."
        exit 1
    fi
    if [[ ! -d ${cal_dir}/hyperdrive ]]; then
        mkdir -p -m 771 ${cal_dir}/hyperdrive
    elif [[ -r ${cal_dir}/hyperdrive/hyperdrive_solutions.bin ]]; then
        ARCHIVE="${cal_dir}/hyperdrive/archived_\$(date +%s)"
        mkdir -p -m 771 \$ARCHIVE
        mv ${cal_dir}/hyperdrive/*solutions* \$ARCHIVE
    fi

    METAFITS=\$(find ${cal_dir}/*.metafits)
    """
}

process birli {
    label 'cpu'
    label 'birli'

    time { 20.minute * task.attempt }

    debug true
    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    maxRetries 2

    input:
    tuple val(calid), val(cal_dir), val(metafits)

    output:
    tuple val(calid), val(cal_dir), val(metafits)

    script:
    """
    set -eux
    which birli

    if [[ -r ${cal_dir}/${calid}_birli.uvfits ]]; then
        echo "Birli files found. Skipping process."
        exit 0
    fi

    birli ${cal_dir}/*ch???*.fits \
        -m ${metafits} \
        -u ${calid}_birli.uvfits \
        --avg-time-res ${params.dt} \
        --avg-freq-res ${params.df}

    cp ${calid}_birli.uvfits ${cal_dir}/${calid}_birli.uvfits
    """
}

process hyperdrive {
    label 'gpu'
    label 'hyperdrive'

    time { 1.hour * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    maxRetries 2
    publishDir "${cal_dir}/hyperdrive", mode: 'copy'

    input:
    tuple val(calid), val(cal_dir), val(metafits)

    output:
    tuple val(calid), val(cal_dir), path("hyperdrive_solutions.fits"), path("hyperdrive_solutions.bin"), path("*.png")
    
    script:
    """
    set -eux
    which hyperdrive

    # Locate the source list from the lookup file
    SRC_LIST_BASE=/pawsey/mwa/software/python3/mwa-reduce/mwa-reduce-git/models
    SRC_LIST_TARGET=\$(grep ${params.source} ${projectDir}/source_lists.txt | awk '{print \$2}')
    SRC_LIST=\${SRC_LIST_BASE}/\${SRC_LIST_TARGET}
    if [[ ! -r \$SRC_LIST ]]; then
        echo "Error: Source list not found."
        exit 1
    fi
    
    # Perform DI calibration
    hyperdrive di-calibrate -s \$SRC_LIST ${flag_arg} -d ${cal_dir}/${calid}_birli.uvfits ${metafits}

    # Plot the amplitudes and phases of the solutions
    hyperdrive solutions-plot -m ${metafits} hyperdrive_solutions.fits

    # Convert to AO format for VCSBeam
    hyperdrive solutions-convert -m ${metafits} hyperdrive_solutions.fits hyperdrive_solutions.bin
    """
}

// Minimum execution requirements: --obsid OBSID --calids CALID1,CALID2 --sources SOURCE
// TODO : allow for multiple sources
workflow {
    Channel
        .from( params.calids.split(',') )
        .map { calid -> [ calid, "${params.vcs_dir}/${params.obsid}/cal/$calid" ] }
        .set { cal_info }

    check_cal_directory(cal_info) | birli | hyperdrive
}
