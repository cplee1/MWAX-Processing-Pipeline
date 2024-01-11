#!/usr/bin/env nextflow

def help_message() {
    log.info """
        |mwax_download.nf: Download VCS observations and calibration observations
        |and move the data into the standard directory structure.
        |
        |USAGE:
        |   mwax_download.nf [OPTIONS]
        |
        |   Note: Space separated lists must be enclosed in quotes.
        |
        |VOLTAGE DOWNLOAD OPTIONS:
        |   --obsid <OBSID>
        |       ObsID of the VCS observation. [no default]
        |   --duration <DURATION>
        |       Length of time to download in seconds. [no default]
        |   --offset <OFFSET>
        |       Offset from the start of the observation in seconds. [no default]
        |   --asvo_api_key <ASVO_API_KEY>
        |       API key corresponding to the user's ASVO account. [no default]
        |
        |CALIBRATOR DOWNLOAD OPTIONS:
        |   --calids <CALIDS>...
        |       Space separated list of ObsIDs of calibrator observations. [no default]
        |
        |FILE MOVING OPTIONS:
        |   --skip_download
        |       Skip download. Instead, move files which are already downloaded.
        |   --asvo_id_obs <ASVO_ID_OBS>
        |       ASVO job ID of the downloaded VCS observation. [no default]
        |   --asvo_id_cals <ASVO_ID_CALS>...
        |       Space separated list of ASVO job IDs of the downloaded calibrator
        |       observations. [no default]
        |
        |PIPELINE OPTIONS:
        |   --help
        |       Print this help information.
        |   --asvo_dir <ASVO_DIR>
        |       Path to where ASVO downloads are stored.
        |       [default: ${params.asvo_dir}]
        |   --vcs_dir <VCS_DIR>
        |       Path to where VCS data files will be stored.
        |       [default: ${params.vcs_dir}]
        |   -w <WORK_DIR>
        |       The Nextflow work directory. Delete the directory once the
        |       process is finished. [default: ${workDir}]
        |
        |EXAMPLES:
        |1. Typical usage
        |   mwax_download.nf --obsid 1372184672 --calids "1372184552 1372189472"
        |   --duration 600 --offset 0 --asvo_api_key <API_KEY>
        |2. Move files which are already downloaded
        |   mwax_download.nf --skip_download --asvo_id_obs 661635 --asvo_id_cals "661634 661636"
        """.stripMargin()
}

if ( params.help ) {
    help_message()
    exit(0)
}

include { dl; mv } from '../modules/download_module'

workflow {
    if ( ! params.obsid && ! params.calids ) {
        println "Please specify obs IDs with --obsid or --calids."
    } else {
        if ( params.skip_download ) {
            if ( ! params.asvo_id_obs && ! params.asvo_id_cals ) {
                println "Please specify ASVO job IDs with --asvo_id_obs and --asvo_id_cals."
            } else {
                mv()  // Move data
            }
        } else {
            if ( ! params.asvo_api_key ) {
                println "Please specify ASVO API key with --asvo_api_key."
            } else {
                if ( ! params.duration && ! params.offset ) {
                    println "Please specify the duration and offset with --duration and --offset."
                } else {
                    dl()  // Download and move data
                }
            }
        }
    }
}
