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
        |   Download options:
        |   --download
        |       Download before beamforming.
        |   --download_only
        |       Download and then exit the pipeline.
        |   --num_dl_jobs
        |       Number of ASVO jobs to split the VCS download into.
        |       [default: ${params.num_dl_jobs}]
        |   --asvo_api_key <ASVO_API_KEY>
        |       API key corresponding to the user's ASVO account.
        |       [default: ${params.asvo_api_key}]
        |
        |   Frequency setup options:
        |   --low_chan <LOW_CHAN>
        |       Index of lowest coarse channel.
        |       [no default]
        |   --num_chan <NUM_CHAN>
        |       Number of coarse channels to process.
        |       [no default]
        |
        |   Calibration solution options:
        |   --use_default_sol
        |       Fetch the calibration solution from the repository.
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
        |   These options are only available for beamforming on pointing coordinates
        |   --acacia_profile <ACACIA_PROFILE>
        |       Profile to upload files to on Acacia.
        |       [no default]
        |   --acacia_bucket <ACACIA_BUCKET>
        |       Bucket to upload files to on Acacia.
        |       [no default]
        |   --acacia_prefix <ACACIA_PREFIX>
        |       Path to the directory within the Acacia bucket where archived files
        |       will be uploaded to.
        |       [no default]
        |
        |   Pipeline options
        |   --help
        |       Print this help information.
        |   -w <WORK_DIR>
        |       The Nextflow work directory. Delete the directory once the
        |       process is finished.
        |       [default: ${workDir}]
        |   --vcs_dir <VCS_DIR>
        |       Path to where VCS data files will be stored.
        |       [default: ${params.vcs_dir}]
        |   --vcsbeam_version <VCSBEAM_VERSION>
        |       The vcsbeam module version to use.
        |       [default: ${params.vcsbeam_version}]
        |
        |~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        |
        |   Examples:
        |
        |   1. Downloading an observation then beamforming in PSRFITS format
        |   mwax_beamform.nf --download --obsid 1372184672 --duration 600 --offset 0
        |                    --low_chan 109 --num_chan 24 --fits
        |                    --calid 1372184552 --use_default_sol
        |                    --psrs "J2039-3616 J2124-3358 J2241-5236"
        |
        |   2. Same as (1) but using VDIF format and providing pointings
        |   mwax_beamform.nf --download --obsid 1372184672 --duration 600 --offset 0
        |                    --low_chan 109 --num_chan 24 --vdif
        |                    --calid 1372184552 --use_default_sol
        |                    --pointings "20:39:16.6_-36:16:17 21:24:43.84_-33:58:45.01"
        |
        |   3. Re-folding beamformed data
        |   mwax_beamform.nf --obsid 1372184672 --duration 600 --fits --skip_bf
        |                    --psrs "J2039-3616 J2124-3358 J2241-5236"
        |
        |   4. Downloaded data without beamforming
        |   mwax_beamform.nf --download_only --obsid 1372184672 --duration 600 --offset 0
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
        |   1. Downloading an observation then beamforming in PSRFITS format
        |   mwax_beamform.nf --download --obsid 1372184672 --duration 592 --offset 0
        |                    --low_chan 109 --num_chan 24 --fits
        |                    --calid 1372184552 --use_default_sol
        |                    --psrs "J2039-3616 J2124-3358 J2241-5236"
        |
        |   2. Same as (1) but using VDIF format and providing pointings
        |   mwax_beamform.nf --download --obsid 1372184672 --duration 592 --offset 0
        |                    --low_chan 109 --num_chan 24 --vdif
        |                    --calid 1372184552 --use_default_sol
        |                    --pointings "20:39:16.6_-36:16:17 21:24:43.84_-33:58:45.01"
        |
        |   3. Re-folding beamformed data
        |   mwax_beamform.nf --obsid 1372184672 --duration 592 --fits --skip_bf
        |                    --psrs "J2039-3616 J2124-3358 J2241-5236"
        |
        |   4. Downloaded data without beamforming
        |   mwax_beamform.nf --download_only --obsid 1372184672 --duration 592 --offset 0
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

include { VCS_BF } from './workflows/vcs_bf'

workflow {
    if (params.download_only) {
        if (params.obsid == null) {
            System.err.println("ERROR: No downloads requested")
        } else {
            if (params.duration == null) {
                System.err.println("ERROR: Observation duration not defined")
            }
            if (params.offset == null) {
                System.err.println("ERROR: Observation offset not defined")
            }
            if (params.duration != null && params.offset != null) {
                VCS_BF()
            }
        }
    } else {
        if (params.obsid == null) {
            System.err.println("ERROR: Obs ID is not defined")
        }
        if (params.duration == null) {
            System.err.println("ERROR: Observation duration not defined")
        }
        if (params.offset == null) {
            System.err.println("ERROR: Observation offset not defined")
        }
        if (!((!params.skip_bf && params.calid != null) || params.skip_bf)) {
            System.err.println("ERROR: Calibrator obs ID is not defined")
        }
        if (!((!params.skip_bf && params.low_chan != null) || params.skip_bf)) {
            System.err.println("ERROR: Lowest coarse channel is not defined")
        }
        if (!((!params.skip_bf && params.num_chan != null) || params.skip_bf)) {
            System.err.println("ERROR: Number of coarse channels is not defined")
        }
        if (params.psrs == null && params.pointings == null && params.pointings_file == null) {
            System.err.println("ERROR: Pulsar(s) or pointing(s) not defined")
        }
        if (params.fits != true && params.vdif != true) {
            System.err.println("ERROR: File format not defined")
        }
        if (params.obsid != null \
            && params.duration != null \
            && params.offset != null \
            && ((!params.skip_bf && params.calid != null) || params.skip_bf) \
            && ((!params.skip_bf && params.low_chan != null) || params.skip_bf) \
            && ((!params.skip_bf && params.num_chan != null) || params.skip_bf) \
            && !(params.psrs == null && params.pointings == null && params.pointings_file == null) \
            && (params.fits == true || params.vdif == true)) {
            VCS_BF()
        }
    }
}
