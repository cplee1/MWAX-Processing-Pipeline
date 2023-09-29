#!/usr/bin/env nextflow

def help_message() {
    log.info """
        |mwax_calibrate.nf: Calibrate using Birli and Hyperdrive.
        |
        |USAGE:
        |   mwax_calibrate.nf [OPTIONS]
        |
        |   Note: Space separated lists must be enclosed in quotes.
        |
        |REQUIRED OPTIONS:
        |   --obsid <OBSID>
        |       ObsID of the VCS observation. [no default]
        |   --calibrators <CALIBRATORS>...
        |       Space separated list of CalID:SOURCE pairs. If the source
        |       is found in the lookup table, will use its specific model.
        |       Otherwise, default to GLEAM-X source catalogue.
        |       e.g. "1234567890:HerA 1234567891:CenA"
        |       Available sources: CenA, Crab, HerA, HydA, PicA, VirA.
        |
        |BIRLI OPTIONS:
        |   --df <DF>
        |       Desired frequency resolution. [default: ${params.df} kHz]
        |   --dt <DT>
        |       Desired time resolution. [default: ${params.dt} s]
        |   --force_birli
        |       Force Birli to run.
        |   --skip_birli
        |       Force Birli not to run. If UVFITS file cannot be found,
        |       pipeline will exit.
        |
        |HYPERDRIVE OPTIONS:
        |   --flagged_tiles <FLAGGED_TILES>...
        |       Space separated list of flagged tiles. [default: none]
        |   --flagged_fine_chans <FLAGGED_FINE_CHANS>
        |       Space separated list of fine channels to flag per coarse channel.
        |       Provide a blank string to disable this option.
        |       [default: ${params.flagged_fine_chans}]
        |   --src_catalogue <SRC_CATALOGUE>
        |       Source catalogue to use if specific calibrator model is not found.
        |       [default: ${params.src_catalogue}]
        |
        |PIPELINE OPTIONS:
        |   --help
        |       Print this help information.
        |   --birli_version <BIRLI_VERSION>
        |       The birli module version to use. [default: ${params.birli_version}]
        |   --hyperdrive_version <HYPERDRIVE_VERSION>
        |       The hyperdrive module version to use. [default: ${params.hyperdrive_version}]
        |   -w <WORK_DIR>
        |       The Nextflow work directory. Delete the directory once the
        |       process is finished.
        |       [default: ${workDir}]
        |
        |EXAMPLES:
        |1. Initial calibration
        |   mwax_calibrate.nf --obsid 1372184672 --calibrators 1372189472:3C444
        |2. Re-calibration after initial inspection
        |   mwax_calibrate.nf --obsid 1372184672 --calibrators 1372189472:3C444
        |   --flagged_tiles "38 52 55 92 93 135" --skip_birli
        |3. Re-calibrate and change frequency downsampling
        |   mwax_calibrate.nf --obsid 1372184672 --calibrators 1372189472:3C444
        |   --flagged_tiles "38 52 55 92 93 135" --df 20 --force_birli
        |   --flagged_fine_chans "0 1 2 3 60 61 62 63"
        """.stripMargin()
}

if ( params.help ) {
    help_message()
    exit(0)
}

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

    shell '/bin/bash', '-veuo', 'pipefail'

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
    src_list_target=\$(grep ${source} ${projectDir}/source_lists.txt | awk '{print \$2}')
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

workflow {
    Channel
        .from( params.calibrators.split(' ') )
        .map { calibrator -> [ calibrator.split(':')[0], "${params.vcs_dir}/${params.obsid}/cal/${calibrator.split(':')[0]}", calibrator.split(':')[1] ] }
        .set { cal_info }

    if ( params.skip_birli ) {
        check_cal_directory(cal_info) | hyperdrive
    } else {
        check_cal_directory(cal_info) | birli | hyperdrive
    }
}