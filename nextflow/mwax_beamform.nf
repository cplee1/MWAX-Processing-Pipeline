#!/usr/bin/env nextflow

def help_message() {
    log.info """
        |mwax_beamform.nf: Beamform and fold VCS pulsar observations.
        |
        |USAGE:
        |   mwax_beamform.nf [OPTIONS]
        |
        |OPTIONS:
        |   --help
        |       Print this help information.
        |   --vcsbeam_version <VCSBEAM_VERSION>
        |       The vcsbeam module version to use. [default: ${params.vcsbeam_version}]
        |   -w <WORK_DIR>
        |       The Nextflow work directory. Delete the directory once the
        |       process is finished. [default: ${workDir}]
        |
        |OBSERVATION:
        |   --obsid <OBSID>
        |       ObsID of the VCS observation. [no default]
        |   --calid <CALID>
        |       ObsID of calibrator. [no default]
        |   --begin <BEGIN>
        |       First GPS time to process. [no default]
        |   --duration <DURATION>
        |       Length of time to process. [default: ${params.duration} s]
        |   --low_chan <LOW_CHAN>
        |       Index of lowest coarse channel. [default: ${params.low_chan}]
        |   --num_chan <NUM_CHAN>
        |       Number of coarse channels to process. [default: ${params.num_chan}]
        |   --flagged_tiles <FLAGGED_TILES>...
        |       Space separated list of flagged tiles (enclosed in quotes if
        |       more than one flag is specified). [default: none]
        |
        |FILE FORMAT (CHOOSE AT LEAST ONE):
        |   --fits
        |       Export beamformed data in PSRFITS format.
        |   --vdif
        |       Export beamformed data in VDIF format.
        |
        |BEAMFORMING:
        |   --psrs <PSRS>...
        |       Space separated list of pulsar J names (enclosed in quotes
        |       if more than one pulsar is specified). [no default]
        |       e.g. "J1440-6344 J1453-6413 J1456-6843"
        |   --skip_bf
        |       Re-fold existing data without re-beamforming.
        |
        |DEDISPERSION AND FOLDING:
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
        |SEARCH/OPTIMISATION:
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
        |EXAMPLES:
        |1. Beamforming
        |   mwax_beamform.nf --obsid 1372184672 --calid 1372184552 --begin 1372186776
        |   --duration 592 --low_chan 109 --num_chan 24 --fits
        |   --flagged_tiles "38 52 55 92 93 135"
        |   --psrs "J2039-3616 J2124-3358 J2241-5236"
        |2. Re-folding beamformed data
        |   mwax_beamform.nf --obsid 1372184672 --duration 592 --fits --skip_bf
        |   --psrs "J2039-3616 J2124-3358 J2241-5236"
        """.stripMargin()
}

if ( params.help ) {
    help_message()
    exit(0)
}

if ( params.fits != true && params.vdif != true ) {
    log.info('No file format selected.')
    exit(1)
}

include { beamform_sp; dspsr_wf; prepfold_wf } from './modules/singlepixel_module'
include { beamform_mp } from './modules/multipixel_module'

workflow {
    if ( params.fits ) {
        if ( params.skip_bf ){
            Channel
                .from(params.psrs.split(' '))
                .set { psrs }
            prepfold_wf(psrs)
        } else {
            Channel
                .from(params.psrs.split(' '))
                .collect()
                .set { psrs }
            beamform_mp(psrs)
        }
    }
    if ( params.vdif ) {
        Channel
            .from(params.psrs.split(' '))
            .set { psrs }
        if ( params.skip_bf ) {
            dspsr_wf(psrs)
        } else {
            beamform_sp(psrs)
        }
    }
}