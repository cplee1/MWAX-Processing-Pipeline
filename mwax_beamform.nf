#!/usr/bin/env nextflow

def help_message() {
    log.info """
        |~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        |
        |   M W A X _ B E A M F O R M . N F
        |
        |~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        |
        |   Beamform and post-process VCS observations.
        |
        |   Usage: mwax_beamform.nf [OPTIONS]
        |
        |~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        |
        |   Required options:
        |   --obsid <OBSID>
        |       ObsID of the VCS observation.
        |       [no default]
        |   --calid <CALID>
        |       ObsID of calibrator.
        |       [no default]
        |   --begin <BEGIN>
        |       First GPS time to process.
        |       [no default]
        |   --duration <DURATION>
        |       Length of time to process in seconds.
        |       [no default]
        |
        |       Output file format (choose AT LEAST ONE of the following):
        |       --fits
        |           Export beamformed data in PSRFITS format.
        |       --vdif
        |           Export beamformed data in VDIF format.
        |
        |       Target selection (choose ONE of the following):
        |       --psrs <PSRS>...
        |           Space separated list of pulsar J names.
        |           e.g. "J1440-6344 J1453-6413 J1456-6843"
        |           [default: none]
        |       --pointings <POINTINGS>...
        |           Space separated list of pointings with the RA and Dec separated
        |           by _ in the format HH:MM:SS_+DD:MM:SS.
        |           e.g. "19:23:48.53_-20:31:52.95 19:23:40.00_-20:31:50.00"
        |           [default: none]
        |       --pointings_file <POINTINGS_FILE>
        |           A file containing pointings with the RA and Dec separated by _
        |           in the format HH:MM:SS_+DD:MM:SS on each line.
        |           e.g. "19:23:48.53_-20:31:52.95\\n19:23:40.00_-20:31:50.00"
        |           [default: none]
        |
        |   Frequency setup options:
        |   --low_chan <LOW_CHAN>
        |       Index of lowest coarse channel.
        |       [default: ${params.low_chan}]
        |   --num_chan <NUM_CHAN>
        |       Number of coarse channels to process.
        |       [default: ${params.num_chan}]
        |
        |   Tile flagging options:
        |   --flagged_tiles <FLAGGED_TILES>...
        |       Space separated list of flagged tiles.
        |       [default: none]
        |   --convert_rts_flags
        |       Convert RTS tile indices to TileNames (use this option if you
        |       are giving tile indices to --flagged_tiles). To use this option
        |       you must provide the CalID using --calid.
        |
        |   Dedispersion and folding options:
        |   --skip_bf
        |       Skip straight to folding without beamforming. (Only use this option
        |       when re-running the pipeline after initial beam formation.)
        |   --nbin <NBIN>
        |       Maximum number of phase bins to fold into.
        |       [default: ${params.nbin}]
        |   --fine_chan <FINE_CHAN>
        |       Amount of fine channelisation (dspsr only).
        |       [default: ${params.fine_chan}]
        |   --tint <TINT>
        |       Length of sub-integrations (dspsr only).
        |       [default: ${params.tint} s]
        |   --ephemeris_dir <EPHEMERIS_DIR>
        |       A directory containing custom ephemerides to take preference
        |       over PSRCAT. Ephemeris files must be named <Jname>.par.
        |       [default: ${params.ephemeris_dir}]
        |
        |   Search/optimisation options:
        |   --nosearch_prepfold
        |       Do not search DM or P/Pdot phase spaces using prepfold.
        |   --nosearch_pdmp
        |       Do not search DM/P phase space using pdmp.
        |   --nsub <NSUB>
        |       Number of frequency sub-bands to use in prepfold search.
        |       [default: ${params.nsub}]
        |   --npart <NPART>
        |       Number of sub-integrations to use in prepfold search.
        |       [default: ${params.npart}]
        |   --pdmp_mc <PDMP_MC>
        |       Number of frequency channels to use in pdmp search.
        |       [default: ${params.pdmp_mc}]
        |   --pdmp_ms <PDMP_MS>
        |       Number of sub-integrations to use in pdmp search.
        |       [default: ${params.pdmp_ms}]
        |
        |   Acacia upload options:
        |   Note: These options are only available for beamforming on pointing coordinates
        |   in VDIF format (i.e. single-beam mode).
        |   --acacia_profile <ACACIA_PROFILE>
        |       Profile to upload files to on Acacia.
        |       [default: ${params.acacia_profile}]
        |   --acacia_bucket <ACACIA_BUCKET>
        |       Bucket to upload files to on Acacia.
        |       [default: ${params.acacia_bucket}]
        |   --acacia_prefix_base <ACACIA_PREFIX_BASE>
        |       Path to the directory within the Acacia bucket where archived files
        |       will be uploaded to under subdirectories labelled by obs ID. If no input
        |       is provided, will not upload to Acacia.
        |       [default: none]
        |
        |   Pipeline options
        |   --help
        |       Print this help information.
        |   -w <WORK_DIR>
        |       The Nextflow work directory. Delete the directory once the
        |       process is finished.
        |       [default: ${workDir}]
        |   --vcsbeam_version <VCSBEAM_VERSION>
        |       The vcsbeam module version to use.
        |       [default: ${params.vcsbeam_version}]
        |
        |~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        |
        |   Examples:
        |
        |   1. Beamforming and folding on known pulsars with prepfold
        |   mwax_beamform.nf --obsid 1372184672 --calid 1372184552 --begin 1372186776
        |   --duration 592 --low_chan 109 --num_chan 24 --fits
        |   --psrs "J2039-3616 J2124-3358 J2241-5236"
        |
        |   2. Re-folding beamformed data
        |   mwax_beamform.nf --obsid 1372184672 --duration 592 --fits --skip_bf
        |   --psrs "J2039-3616 J2124-3358 J2241-5236"
        |
        |   3. Beamforming on pointings
        |   mwax_beamform.nf --obsid 1372184672 --calid 1372184552 --begin 1372186776
        |   --duration 592 --low_chan 109 --num_chan 24 --fits
        |   --pointings "20:39:16.6_-36:16:17 21:24:43.846081_-33:58:45.01036"
        |
        |~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        """.stripMargin()
}

def examples_message() {
    log.info """
        |~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        |
        |   M W A X _ B E A M F O R M . N F
        |
        |~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        |
        |   Examples:
        |
        |   1. Beamforming and folding on known pulsars with prepfold
        |   mwax_beamform.nf --obsid 1372184672 --calid 1372184552 --begin 1372186776
        |   --duration 592 --low_chan 109 --num_chan 24 --fits
        |   --psrs "J2039-3616 J2124-3358 J2241-5236"
        |
        |   2. Re-folding beamformed data
        |   mwax_beamform.nf --obsid 1372184672 --duration 592 --fits --skip_bf
        |   --psrs "J2039-3616 J2124-3358 J2241-5236"
        |
        |   3. Beamforming on pointings
        |   mwax_beamform.nf --obsid 1372184672 --calid 1372184552 --begin 1372186776
        |   --duration 592 --low_chan 109 --num_chan 24 --fits
        |   --pointings "20:39:16.6_-36:16:17 21:24:43.846081_-33:58:45.01036"
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

include { bf } from './workflows/beamform'

workflow {
    if ( ! params.obsid ) {
        System.err.println("ERROR: Obs ID is not defined")
    }
    if ( ! params.duration ) {
        System.err.println("ERROR: Observation duration not defined")
    }
    if ( ! ( params.psrs || params.pointings || params.pointings_file ) ) {
        System.err.println("ERROR: Pulsar(s) or pointing(s) not defined")
    }
    if ( params.fits != true && params.vdif != true ) {
        System.err.println("ERROR: File format not defined")
    }
    if ( params.obsid && params.duration && ( params.psrs || params.pointings || params.pointings_file ) && ( params.fits == true || params.vdif == true ) ) {
        if ( params.skip_bf ) {
            bf()  // Dedisperse and fold
        } else {
            if ( ! params.calid ) {
                System.err.println("ERROR: Cal ID is not defined")
            }
            if ( ! params.begin ) {
                System.err.println("ERROR: Observation start GPS time is not defined")
            }
            if ( params.calid && params.begin ) {
                bf()  // Beamform, dedisperse, and fold
            }
        }
    }
}
