#!/usr/bin/env nextflow

def help_message() {
    log.info """
        |mwax_beamform.nf: Beamform and fold VCS pulsar observations.
        |
        |USAGE:
        |   mwax_beamform.nf [OPTIONS]
        |
        |   Note: Space separated lists must be enclosed in quotes.
        |
        |REQUIRED OPTIONS:
        |   --obsid <OBSID>
        |       ObsID of the VCS observation. [no default]
        |   --calid <CALID>
        |       ObsID of calibrator. [no default]
        |   --begin <BEGIN>
        |       First GPS time to process. [no default]
        |   --duration <DURATION>
        |       Length of time to process in seconds. [no default]
        |
        |   Output file format (choose AT LEAST ONE of the following)
        |   --fits
        |       Export beamformed data in PSRFITS format.
        |   --vdif
        |       Export beamformed data in VDIF format.
        |
        |   Target selection (choose ONE of the following)
        |   --psrs <PSRS>...
        |       Space separated list of pulsar J names. [default: none]
        |       e.g. "J1440-6344 J1453-6413 J1456-6843"
        |   --pointings <POINTINGS>...
        |       Space separated list of pointings with the RA and Dec separated
        |       by _ in the format HH:MM:SS_+DD:MM:SS. [default: none]
        |       e.g. "19:23:48.53_-20:31:52.95 19:23:40.00_-20:31:50.00"
        |   --pointings_file <POINTINGS_FILE>
        |       A file containing pointings with the RA and Dec separated by _
        |       in the format HH:MM:SS_+DD:MM:SS on each line. [default: none]
        |       e.g. "19:23:48.53_-20:31:52.95\\n19:23:40.00_-20:31:50.00"
        |
        |FREQUENCY SETUP OPTIONS:
        |   --low_chan <LOW_CHAN>
        |       Index of lowest coarse channel. [default: ${params.low_chan}]
        |   --num_chan <NUM_CHAN>
        |       Number of coarse channels to process. [default: ${params.num_chan}]
        |
        |TILE FLAGGING OPTIONS:
        |   --flagged_tiles <FLAGGED_TILES>...
        |       Space separated list of flagged tiles. [default: none]
        |   --convert_rts_flags
        |       Convert RTS tile indices to TileNames (use this option if you
        |       are giving tile indices to --flagged_tiles). To use this option
        |       you must provide the CalID using --calid.
        |
        |DEDISPERSION AND FOLDING OPTIONS:
        |   --skip_bf
        |       Skip straight to folding without beamforming. (Only use this option
        |       when re-running the pipeline after initial beam formation.)
        |   --nbin <NBIN>
        |       Maximum number of phase bins to fold into. [default: ${params.nbin}]
        |   --fine_chan <FINE_CHAN>
        |       Amount of fine channelisation (dspsr only). [default: ${params.fine_chan}]
        |   --tint <TINT>
        |       Length of sub-integrations (dspsr only). [default: ${params.tint} s]
        |   --ephemeris_dir <EPHEMERIS_DIR>
        |       A directory containing custom ephemerides to take preference
        |       over PSRCAT. Ephemeris files must be named <Jname>.par.
        |       [default: ${params.ephemeris_dir}]
        |
        |SEARCH/OPTIMISATION OPTIONS:
        |   --nosearch_prepfold
        |       Do not search DM or P/Pdot phase spaces using prepfold.
        |   --nosearch_pdmp
        |       Do not search DM/P phase space using pdmp.
        |   --nsub <NSUB>
        |       Number of frequency sub-bands to use in prepfold search. [default: ${params.nsub}]
        |   --npart <NPART>
        |       Number of sub-integrations to use in prepfold search. [default: ${params.npart}]
        |   --pdmp_mc <PDMP_MC>
        |       Number of frequency channels to use in pdmp search. [default: ${params.pdmp_mc}]
        |   --pdmp_ms <PDMP_MS>
        |       Number of sub-integrations to use in pdmp search. [default: ${params.pdmp_ms}]
        |
        |ACACIA OPTIONS:
        |   Note: These options are only available for beamforming on pointing coordinates
        |   in VDIF format (i.e. single-beam mode).
        |   --acacia_profile <ACACIA_PROFILE>
        |       Profile to upload files to on Acacia. [default: ${params.acacia_profile}]
        |   --acacia_bucket <ACACIA_BUCKET>
        |       Bucket to upload files to on Acacia.  [default: ${params.acacia_bucket}]
        |   --acacia_prefix_base <ACACIA_PREFIX_BASE>
        |       Path to the directory within the Acacia bucket where archived files
        |       will be uploaded to under subdirectories labelled by obs ID. If no input
        |       is provided, will not upload to Acacia. [default: none]
        |
        |PIPELINE OPTIONS:
        |   --help
        |       Print this help information.
        |   --vcsbeam_version <VCSBEAM_VERSION>
        |       The vcsbeam module version to use. [default: ${params.vcsbeam_version}]
        |   -w <WORK_DIR>
        |       The Nextflow work directory. Delete the directory once the
        |       process is finished. [default: ${workDir}]
        |
        |EXAMPLES:
        |1. Beamforming and folding on known pulsars
        |   mwax_beamform.nf --obsid 1372184672 --calid 1372184552 --begin 1372186776
        |   --duration 592 --low_chan 109 --num_chan 24 --fits
        |   --flagged_tiles "38 52 55 92 93 135" --convert_rts_flags
        |   --psrs "J2039-3616 J2124-3358 J2241-5236"
        |2. Re-folding beamformed data
        |   mwax_beamform.nf --obsid 1372184672 --duration 592 --fits --skip_bf
        |   --psrs "J2039-3616 J2124-3358 J2241-5236"
        |3. Beamforming on pointings
        |   mwax_beamform.nf --obsid 1372184672 --calid 1372184552 --begin 1372186776
        |   --duration 592 --low_chan 109 --num_chan 24 --fits
        |   --flagged_tiles "38 52 55 92 93 135"  --convert_rts_flags
        |   --pointings "20:39:16.6_-36:16:17 21:24:43.846081_-33:58:45.01036"
        """.stripMargin()
}

if ( params.help ) {
    help_message()
    exit(0)
}

include { spsr; spt; dspsr_wf; prepfold_wf } from './modules/singlepixel_module'
include { mpsr; mpt } from './modules/multipixel_module'

workflow bf {
    if ( params.psrs ) {
        // Beamform and fold/search catalogued pulsars
        Channel.from(params.psrs.split(' '))
            | set { psrs }
        
        if ( params.fits ) {
            if ( params.skip_bf ) {
                prepfold_wf(psrs)  // Fold PSRFITS data
            } else {
                psrs | collect | mpsr  // Multipixel beamform on pulsars
            }
        }
        if ( params.vdif ) {
            if ( params.skip_bf ) {
                dspsr_wf(psrs)  // Fold VDIF data
            } else {
                spsr(psrs)  // Singlepixel beamform on pulsars
            }
        }
    } else if ( params.pointings ||  params.pointings_file ) {
        // Beamform on pointings
        if ( params.pointings ) {
            // Get pointings from command line input
            Channel.from(params.pointings.split(' '))
                | map { pointing -> [ pointing.split('_')[0], pointing.split('_')[1] ] }
                | set { pointings }
        } else if ( params.pointings_file ) {
            // Get pointings from file
            Channel.fromPath(params.pointings_file)
                | splitCsv
                | flatten
                | map { pointing -> [ pointing.split('_')[0], pointing.split('_')[1] ] }
                | set { pointings }
        }
        if ( params.skip_bf ) {
            log.info('Custom pointings are not folded, and thus not compatible with --skip_bf. Exiting.')
            exit(1)
        } else {
            if ( params.fits ) {
                mpt(pointings)  // Multipixel beamform on pointings
            }
            if ( params.vdif ) {
                spt(pointings)  // Singlepixel beamform on pointings
            }
        }
    } else {
        log.info('No pulsars or pointings specified. Exiting.')
        exit(1)
    }
}

workflow {
    if ( ! params.obsid ) {
        println "Please provide the obs ID with --obsid."
    } else if ( ! params.duration ) {
        println "Please provide the duration with --duration."
    } else if ( ! params.psrs && ! params.pointings && ! params.pointings_file ) {
        println "Please provide targets with --psrs or --pointings or --pointings_file."
    } else if ( params.fits != true && params.vdif != true ) {
        println "Please specify the file format(s) to use with --fits or --vdif."
    } else {
        if ( params.skip_bf ) {
            bf()  // Dedisperse and fold
        } else {
            if ( ! params.calid ) {
                println "Please provide the obs ID of a calibrator observation with --calid."
            } else if ( ! params.begin ) {
                println "Please provide the begin time of the observation with --begin."
            } else {
                bf()  // Beamform, dedisperse, and fold
            }
        }
    }
}