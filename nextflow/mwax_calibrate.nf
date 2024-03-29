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
        |       Space separated list of CalID:SOURCE pairs. For multi-component
        |       sources in the lookup table, the specific model will be used.
        |       The available models are: CenA, Crab, HerA, HydA, PicA, VirA.
        |       Otherwise, the default is to use the GLEAM catalogue.
        |       e.g. To use specfic model: "1234567890:HerA 1234567891:CenA"
        |       e.g. To just use GLEAM: "1234567890:- 1234567891:-"
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

include { cal; cal_jobs } from './modules/calibrate_module'

workflow {
    if ( params.cal_joblist ) {
        cal_jobs("${launchDir}/${params.cal_joblist}") | view
    } else {
        if ( ! params.obsid ) {
            println "Please specify the obs ID with --obsid."
        } else if ( ! params.calibrators ) {
            println "Please specify the calibrator(s) with --calibrators."
        } else {
            cal(params.obsid) | view
        }
    }
}