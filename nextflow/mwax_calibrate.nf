#!/usr/bin/env nextflow

params.help = false
if ( params.help ) {
    help = """mwax_calibrate.nf: Calibrate using Birli and Hyperdrive.
             |Required arguments:
             |  --obsid <OBSID>    Observation ID of the VCS observation [no default]
             |  --calibrators <CALIBRATORS>...
             |                     Space separated list of CalID:Source pairs (enclosed in
             |                     quotes if more than one pair is specified), e.g.
             |                     "12345678:HerA 12345678:CenA"
             |                     Will search source_lists.txt for a dedicated source list.
             |                     If not found, will default to the GLEAM-X catalogue.
             |
             |Downsampling options:
             |  --df <DF>          Desired frequency resolution [default: ${params.df} kHz]
             |  --dt <DT>          Desired time resolution [default: ${params.dt} s]
             |
             |Flagging options:
             |  --flagged_tiles <FLAGGED_TILES>...
             |                     Space separated list of flagged tiles (enclosed in
             |                     quotes if more than one flag is specified) [default: none]
             |  --flag_edge_chans  Number of fine channels to flag at coars channel edges [default: 0]
             |
             |Optional arguments:
             |  --force_birli      Force Birli to regenerate the downsampled UVFITS file
             |  --birli_version    The birli module version to use [default: ${params.birli_version}]
             |  --hyperdrive_version
             |                     The hyperdrive module version to use [default: ${params.hyperdrive_version}]
             |  -w                 The Nextflow work directory. Delete the directory once the
             |                     process is finished [default: ${workDir}]""".stripMargin()
    println(help)
    exit(0)
}

if ( params.flagged_tiles != '' )
    flag_arg = "--tile-flags ${params.flagged_tiles}"
else
    flag_arg = ''

process check_cal_directory {
    input:
    tuple val(calid), val(cal_dir), val(source)

    output:
    tuple val(calid), val(cal_dir), env(METAFITS), val(source)

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

    time { 1.hour * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    maxRetries 2

    input:
    tuple val(calid), val(cal_dir), val(metafits), val(source)

    output:
    tuple val(calid), val(cal_dir), val(metafits), val(source)

    script:
    """
    set -eux
    which birli

    if [[ -r ${cal_dir}/${calid}_birli.uvfits && ${params.force_birli} == 'false' ]]; then
        echo "Birli files found. Skipping process."
        exit 0
    fi

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
    set -eux
    which hyperdrive

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
