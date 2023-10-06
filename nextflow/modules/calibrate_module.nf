#!/usr/bin/env nextflow

if ( params.flagged_tiles != '' )
    tile_flag_opt = "--tile-flags ${params.flagged_tiles}"
else
    tile_flag_opt = ''

if ( params.flagged_fine_chans != '' )
    chan_flag_opt = "--fine-chan-flags-per-coarse-chan ${params.flagged_fine_chans}"
else
    chan_flag_opt = ''

process check_cal_directory {
    shell '/bin/bash', '-veuo', 'pipefail'

    input:
    tuple val(calid), val(cal_dir), val(source)

    output:
    tuple val(calid), val(cal_dir), env(metafits), val(source)

    script:
    """
    if [[ ! -d ${cal_dir} ]]; then
        echo "Error: Cannot locate calibration directory ${cal_dir}."
        exit 1
    fi
    if [[ ! -d ${cal_dir}/hyperdrive ]]; then
        mkdir -p -m 771 ${cal_dir}/hyperdrive
    elif [[ -r ${cal_dir}/hyperdrive/hyperdrive_solutions.bin ]]; then
        archive="${cal_dir}/hyperdrive/archived_\$(date +%s)"
        mkdir -p -m 771 \$archive
        mv ${cal_dir}/hyperdrive/*solutions* \$archive
    fi

    metafits=\$(find ${cal_dir}/*.metafits)

    if [[ \$(echo \$metafits | wc -l) -ne 1 ]]; then
        echo "Error: Unique metafits file not found."
        exit 1
    fi
    """
}

process birli {
    label 'cpu'
    label 'birli'

    shell '/bin/bash', '-veuo', 'pipefail'

    time { 1.hour * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    maxRetries 1

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
    birli \
        --metafits ${metafits} \
        --avg-time-res ${params.dt} \
        --avg-freq-res ${params.df} \
        --uvfits-out ${calid}_birli.uvfits \
        ${cal_dir}/*ch???*.fits

    cp ${calid}_birli.uvfits ${cal_dir}/${calid}_birli.uvfits
    """
}

process hyperdrive {
    label 'gpu'
    label 'hyperdrive'

    shell '/bin/bash', '-veu'

    time { 30.minute * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    maxRetries 1
    publishDir "${cal_dir}/hyperdrive", mode: 'copy'

    input:
    tuple val(calid), val(cal_dir), val(metafits), val(source)

    output:
    tuple val(calid), val(cal_dir), path("hyperdrive_solutions.fits"), path("hyperdrive_solutions.bin"), path("*.png")
    
    script:
    """
    hyperdrive -V

    if [[ ! -r ${cal_dir}/${calid}_birli.uvfits ]]; then
        echo "Error: readable UVFITS file not found."
        exit 1
    fi

    # Check lookup table for a specific model
    src_list_target=\$(grep ${source} ${projectDir}/../source_lists.txt | awk '{print \$2}')
    src_list_base=${params.models_dir}
    src_list=\${src_list_base}/\${src_list_target}

    if [[ ! -z \$src_list_target && -r \$src_list ]]; then
        echo "Using specific model \$src_list"
    else
        echo "Creating list of 1000 brightest sources from catalogue."
        src_list_catalogue=${params.src_catalogue}
        src_list=srclist_1000.yaml

        hyperdrive srclist-by-beam \
            --metafits ${metafits} \
            --number 1000 \
            \$src_list_catalogue \
            \$src_list
    fi

    # Perform DI calibration
    hyperdrive di-calibrate \
        --source-list \$src_list \
        --data ${cal_dir}/${calid}_birli.uvfits ${metafits} \
        ${tile_flag_opt} \
        ${chan_flag_opt}

    # Plot the amplitudes and phases of the solutions
    hyperdrive solutions-plot \
        --metafits ${metafits} \
        hyperdrive_solutions.fits

    # Convert to Offringa format for VCSBeam
    hyperdrive solutions-convert \
        --metafits ${metafits} \
        hyperdrive_solutions.fits \
        hyperdrive_solutions.bin
    """
}

workflow cal {
    take:
        obsid
    main:
        Channel
            .from( params.calibrators.split(' ') )
            .map { calibrator -> [ calibrator.split(':')[0], "${params.vcs_dir}/${obsid}/cal/${calibrator.split(':')[0]}", calibrator.split(':')[1] ] }
            .set { cal_info }

        if ( params.skip_birli ) {
            check_cal_directory(cal_info) | hyperdrive | map { it[1] } | set { cal_dirs }
        } else {
            check_cal_directory(cal_info) | birli | hyperdrive | map { it[1] } | set { cal_dirs }
        }
    emit:
        cal_dirs
}