#!/usr/bin/env nextflow

def help_message() {
    log.info """
        |~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        |
        |   M W A X _ D O W N L O A D . N F
        |
        |~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        |
        |   Download observations for VCS processing and store them in a the
        |   standard VCS directory structure.
        |
        |   Ensure that the ASVO API key is defined in your login environment.
        |
        |   Usage: mwax_download.nf [OPTIONS]
        |
        |~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        |
        |   VCS download options:
        |   --obsid <OBSID>
        |       ObsID of the VCS observation.
        |       [no default]
        |   --duration <DURATION>
        |       Length of time to download in seconds.
        |       [no default]
        |   --offset <OFFSET>
        |       Offset from the start of the observation in seconds.
        |       [no default]
        |   --asvo_api_key <ASVO_API_KEY>
        |       API key corresponding to the user's ASVO account.
        |       [default: ${params.asvo_api_key}]
        |
        |   Calibration download options:
        |   --calids <CALIDS>...
        |       Space separated list of ObsIDs of calibrator observations.
        |        Must enclose list in quotes.
        |       [no default]
        |
        |   File moving options:
        |   --skip_download
        |       Skip download. Instead, move files which are already downloaded.
        |   --asvo_id_obs <ASVO_ID_OBS>
        |       ASVO job ID of the downloaded VCS observation.
        |       [no default]
        |   --asvo_id_cals <ASVO_ID_CALS>...
        |       Space separated list of ASVO job IDs of the downloaded calibrator
        |       observations. Must enclose list in quotes.
        |       [no default]
        |
        |   Pipeline options:
        |   --help
        |       Print this help information.
        |   -w <WORK_DIR>
        |       The Nextflow work directory.
        |       [default: ${workDir}]
        |   --asvo_dir <ASVO_DIR>
        |       Path to where ASVO downloads are stored.
        |       [default: ${params.asvo_dir}]
        |   --vcs_dir <VCS_DIR>
        |       Path to where VCS data files will be stored.
        |       [default: ${params.vcs_dir}]
        |
        |~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        |
        |   Examples:
        |
        |   1. VCS download only:
        |   mwax_download.nf --obsid 1372184672 --duration 600 --offset 0
        |
        |   2. VCS and calibrator download:
        |   mwax_download.nf --obsid 1372184672 --duration 600 --offset 0
        |   --calids "1372184552 1372189472"
        |
        |   3. Move downloaded VCS files:
        |   mwax_download.nf --skip_download --asvo_id_obs 661635
        |
        |   4. Move downloaded VCS and calibrator files:
        |   mwax_download.nf --skip_download --asvo_id_obs 661635
        |   --asvo_id_cals "661634 661636"
        |
        |~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        """.stripMargin()
}

def examples_message() {
    log.info """
        |~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        |
        |   M W A X _ D O W N L O A D . N F
        |
        |~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        |
        |   Examples:
        |
        |   1. VCS download only:
        |   mwax_download.nf --obsid 1372184672 --duration 600 --offset 0
        |
        |   2. VCS and calibrator download:
        |   mwax_download.nf --obsid 1372184672 --duration 600 --offset 0
        |   --calids "1372184552 1372189472"
        |
        |   3. Move downloaded VCS files:
        |   mwax_download.nf --skip_download --asvo_id_obs 661635
        |
        |   4. Move downloaded VCS and calibrator files:
        |   mwax_download.nf --skip_download --asvo_id_obs 661635
        |   --asvo_id_cals "661634 661636"
        |
        |~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        """.stripMargin()
}

if ( params.help ) {
    help_message()
    exit(0)
} else if ( params.examples ) {
    examples_message()
    exit(0)
}

include { mv } from './workflows/download'
include { dl } from './workflows/download'

workflow {
    if ( params.skip_download ) {
        if ( ! params.asvo_id_obs && ! params.asvo_id_cals ) {
            System.err.println("ERROR: ASVO job ID(s) not defined")
        } 
        if ( ! params.asvo_dir ) {
            System.err.println("ERROR: ASVO directory is not defined")
        }
        if ( ! params.obsid ) {
            System.err.println("ERROR: Obs ID is not defined")
        }
        if ( params.asvo_dir && params.obsid && ( params.asvo_id_obs || params.asvo_id_cals ) ) {
            mv()  // Move data
        }
    } else {
        if ( ! params.obsid ) {
            System.err.println("ERROR: Obs ID is not defined")
        }
        if ( ! params.duration ) {
            System.err.println("ERROR: Observation duration not defined")
        }
        if ( ! params.offset ) {
            System.err.println("ERROR: Observation time offset not defined")
        }
        if ( params.asvo_api_key && params.duration && params.offset ) {
            dl()  // Download and move data
        }
    }
}
