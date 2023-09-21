#!/usr/bin/env nextflow

def help_message() {
    log.info """
        |mwax_calibrate.nf: Calibrate using Birli and Hyperdrive.
        |
        |USAGE:
        |   mwax_calibrate.nf [OPTIONS]
        |
        |OPTIONS:
        |   --help
        |       Print this help information.
        |   --birli_version <BIRLI_VERSION>
        |       The birli module version to use. [default: ${params.birli_version}]
        |   --hyperdrive_version <HYPERDRIVE_VERSION>
        |       The hyperdrive module version to use. [default: ${params.hyperdrive_version}]
        |   -w <WORK_DIR>
        |       The Nextflow work directory. Delete the directory once the
        |       process is finished. [default: ${workDir}]
        |
        |OBSERVATION:
        |   --obsid <OBSID>
        |       ObsID of the VCS observation. [no default]
        |   --calibrators <CALIBRATORS>...
        |       Space separated list of CalID:SOURCE pairs (enclosed in
        |       quotes if more than one pair is specified). If the source
        |       is not found in the lookup table, will default to GLEAM-X.
        |       e.g. "1234567890:HerA 1234567891:CenA"
        |
        |BIRLI:
        |   --df <DF>
        |       Desired frequency resolution. [default: ${params.df} kHz]
        |   --dt <DT>
        |       Desired time resolution. [default: ${params.dt} s]
        |   --flag_edge_chans <FLAG_EDGE_CHANS>
        |       Number of fine channels to flag at coarse channel edges.
        |       (Usually 1 or 2 is enough). [default: ${params.flag_edge_chans}]
        |   --force_birli
        |       Force Birli to regenerate the downsampled UVFITS file.
        |
        |HYPERDRIVE:
        |   --flagged_tiles <FLAGGED_TILES>...
        |       Space separated list of flagged tiles (enclosed in quotes
        |       if more than one flag is specified). [default: none]
        |
        |EXAMPLES:
        |1. Initial calibration
        |   mwax_calibrate.nf --obsid 1372184672 --calibrators 1372189472:3C444
        |2. Re-calibration after initial inspection
        |   mwax_calibrate.nf --obsid 1372184672 --calibrators 1372189472:3C444
        |   --flagged_tiles "38 52 55 92 93 135" --flag_edge_chans 1 --force_birli
        """.stripMargin()
}

if ( params.help ) {
    help_message()
    exit(0)
}

if ( params.flagged_tiles != '' )
    flag_arg = "--tile-flags ${params.flagged_tiles}"
else
    flag_arg = ''

process check_cal_directory {
    shell '/bin/bash', '-veuo', 'pipefail'

    input:
    tuple val(calid), val(cal_dir), val(source)

    output:
    tuple val(calid), val(cal_dir), env(METAFITS), val(source)

    script:
    """
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

    shell '/bin/bash', '-veuo', 'pipefail'

    time { 1.hour * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    maxRetries 2

    input:
    tuple val(calid), val(cal_dir), val(metafits), val(source)

    output:
    tuple val(calid), val(cal_dir), val(metafits), val(source)

    script:
    """
    if [[ -r ${cal_dir}/${calid}_birli.uvfits && ${params.force_birli} == 'false' ]]; then
        echo "Birli files found. Skipping process."
        exit 0
    fi

    birli -V
    birli ${cal_dir}/*ch???*.fits \
        -m ${metafits} \
        -u ${calid}_birli.uvfits \
        --avg-time-res ${params.dt} \
        --avg-freq-res ${params.df} \
        --flag-edge-chans ${params.flag_edge_chans}

    cp ${calid}_birli.uvfits ${cal_dir}/${calid}_birli.uvfits
    """
}

process hyperdrive {
    label 'gpu'
    label 'hyperdrive'

    shell '/bin/bash', '-veuo', 'pipefail'

    time { 1.hour * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    maxRetries 2
    publishDir "${cal_dir}/hyperdrive", mode: 'copy'

    input:
    tuple val(calid), val(cal_dir), val(metafits), val(source)

    output:
    tuple val(calid), val(cal_dir), path("hyperdrive_solutions.fits"), path("hyperdrive_solutions.bin"), path("*.png")
    
    script:
    """
    # Locate the source list
    SRC_LIST_TARGET=\$(grep ${source} ${projectDir}/source_lists.txt | awk '{print \$2}')
    SRC_LIST_BASE=/pawsey/mwa/software/python3/mwa-reduce/mwa-reduce-git/models
    SRC_LIST=\${SRC_LIST_BASE}/\${SRC_LIST_TARGET}
    if [[ -z \$SRC_LIST_TARGET ]]; then
        echo "Error: Source list not found in lookup table. Using GGSM catalogue."
        #SRC_LIST_CATALOGUE=/pawsey/mwa/software/python3/srclists/master/srclist_pumav3_EoR0aegean_fixedEoR1pietro+ForA_phase1+2.txt
        SRC_LIST_CATALOGUE=/astro/mwavcs/cplee/remote_backup/source_lists/GGSM_updated.txt
        SRC_LIST=srclist_1000.yaml
        hyperdrive srclist-by-beam -n 1000 -m ${metafits} \$SRC_LIST_CATALOGUE \$SRC_LIST
    fi
    
    # Perform DI calibration
    hyperdrive di-calibrate -V
    hyperdrive di-calibrate -s \$SRC_LIST ${flag_arg} -d ${cal_dir}/${calid}_birli.uvfits ${metafits}

    # Plot the amplitudes and phases of the solutions
    hyperdrive solutions-plot -m ${metafits} hyperdrive_solutions.fits

    # Convert to AO format for VCSBeam
    hyperdrive solutions-convert -m ${metafits} hyperdrive_solutions.fits hyperdrive_solutions.bin
    """
}

workflow {
    Channel
        .from( params.calibrators.split(' ') )
        .map { calibrator -> [ calibrator.split(':')[0], "${params.vcs_dir}/${params.obsid}/cal/${calibrator.split(':')[0]}", calibrator.split(':')[1] ] }
        .set { cal_info }

    check_cal_directory(cal_info) | birli | hyperdrive
}