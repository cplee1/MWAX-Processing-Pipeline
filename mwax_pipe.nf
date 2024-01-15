#!/usr/bin/env nextflow

def help_message() {
    log.info """
        |~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        |
        |   M W A X _ P I P E . N F
        |
        |~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        |
        |   End-to-end pipeline for processing MWA VCS observations.
        |
        |   Combines mwax_calibrate.nf and mwax_beamform.nf and assumes that the
        |   calibration solution is available in the repository.
        |
        |   Ensure that your ASVO API key is defined in your login environment.
        |
        |   Usage: mwax_pipe.nf [OPTIONS]
        |
        |~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        |
        |   Not all options are listed. See mwax_calibrate.nf and mwax_beamform.nf help
        |   menus for a complete list of options.
        |
        |~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        |
        |   Required options
        |   ----------------
        |
        |   VCS observation:
        |   --obsid <OBSID> :: Observation ID
        |   --duration <DURATION> :: Observation duration to process
        |   --offset <OFFSET> :: Offset from start of observation
        |   --begin <BEGIN> :: Actual start time of data to process
        |
        |   Calibration:
        |   --calid <CALID> :: Calibration ID from solutions repository
        |
        |   Source (choose one):
        |   --psrs <PSRS>... :: List of pulsars to process
        |   --pointings <POINTINGS>... :: List of pointings to process
        |   --pointings_file <POINTINGS_FILE> :: File containing pointings
        |
        |   Output:
        |   --fits :: Process PSRFITS/Stokes and use prepfold
        |   --vdif :: Process VDIF/voltages and use dspsr/pdmp
        |
        |   Options with defaults / not required
        |   ------------------------------------
        |   --low_chan <LOW_CHAN> :: Lowest coarse channel index [Def.: 109]
        |   --num_chan <NUM_CHAN> :: Number of coarse channels [Def.: 24]
        |   --flagged_tiles <FLAGGED_TILES>... :: Additional tiles to flag [Def.: None]
        |   --convert_rts_flags :: Convert RTS tile indices to tile names
        |
        |~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        """.stripMargin()
}

if ( params.help ) {
    help_message()
    exit(0)
}

include { pipe } from './workflows/pipeline'

workflow {
    if ( params.obsid == null ) {
        System.err.println("ERROR: Obs ID is not defined")
    }
    if ( params.duration == null ) {
        System.err.println("ERROR: Observation duration not defined")
    }
    if ( params.offset == null ) {
        System.err.println("ERROR: Observation time offset not defined")
    }
    if ( params.begin == null ) {
        System.err.println("ERROR: Observation start GPS time is not defined")
    }
    if ( params.calid == null ) {
        System.err.println("ERROR: Cal ID is not defined")
    }
    if ( params.psrs == null && params.pointings == null && params.pointings_file == null ) {
        System.err.println("ERROR: Pulsar(s) or pointing(s) not defined")
    }
    if ( params.fits != true && params.vdif != true ) {
        System.err.println("ERROR: File format not defined")
    }
    if ( params.obsid != null \
         && params.duration != null \
         && params.offset != null \
         && params.begin != null \
         && params.calid != null \
         && ( params.psrs != null || params.pointings != null || params.pointings_file != null ) \
         && ( params.fits == true || params.vdif == true ) ) {
        // Run pipeline
        pipe()
    }
}
