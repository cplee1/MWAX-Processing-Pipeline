#!/usr/bin/env nextflow

def help_message() {
    log.info """
        |mwax_download_and_calibrate.nf Download VCS observations and calibration
        |observations, move the data into the standard directory structue, and
        |calibrate using Birli and Hyperdrive.
        |
        |USAGE:
        |   mwax_download_and_calibrate.nf [OPTIONS]
        |
        |   Note: Space separated lists must be enclosed in quotes.
        |
        |REQUIRED OPTIONS:
        |   --obsid <OBSID>
        |       ObsID of the VCS observation. [no default]
        |   --duration <DURATION>
        |       Length of time to download in seconds. [no default]
        |   --offset <OFFSET>
        |       Offset from the start of the observation in seconds. [no default]
        |   --asvo_api_key <ASVO_API_KEY>
        |       API key corresponding to the user's ASVO account. [no default]
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
        |   --vcs_dir <VCS_DIR>
        |       Path to where VCS data files will be stored.
        |       [default: ${params.vcs_dir}]
        |   -w <WORK_DIR>
        |       The Nextflow work directory. Delete the directory once the
        |       process is finished.
        |       [default: ${workDir}]
        |
        |EXAMPLES:
        |1. Typical usage
        |   mwax_calibrate.nf --obsid 1372184672 --calibrators "1372184552:3C444 1372189472:3C444"
        |   --duration 600 --offset 0 --asvo_api_key <API_KEY>
        """.stripMargin()
}

if ( params.help ) {
    help_message()
    exit(0)
}

include { dl } from './modules/download_module'
include { cal } from './modules/calibrate_module'

workflow{
    if ( ! params.obsid ) {
        println "Please specify obs ID with --obsid."
    } else if ( ! params.calibrators ) {
        println "Please specify calibrator(s) with --calibrators."
    } else if ( ! params.duration && ! params.offset ) {
        println "Please specify the duration and offset with --duration and --offset."
    } else if ( ! params.asvo_api_key ) {
        println "Please specify ASVO API key with --asvo_api_key."
    } else if ( params.skip_birli == true ) {
        println "Option --skip_birli is not compatible with this workflow."
    } else {
        dl | cal | view  // Download, move, and calibrate data
    }
}