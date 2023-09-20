#!/usr/bin/env nextflow

params.help = false
if ( params.help ) {
    help = """mwax_beamform.nf: Beamform on MWAX VCS data.
             |Required arguments:
             |  --obsid <OBSID>    Observation ID you want to process [no default]
             |  --calid <CALID>    Observation ID of calibrator you want to process [no default]
             |  --begin <BEGIN>    First GPS time to process [no default]
             |  --duration <DURATION>
             |                     Duration of time to process [default: ${params.duration} s]
             |  --low_chan <LOW_CHAN>
             |                     Index of lowest coarse channel [default: ${params.low_chan}]
             |  --num_chan <NUM_CHAN>
             |                     Number of coarse channels to process [default: ${params.num_chan}]
             |  --flagged_tiles <FLAGGED_TILES>...
             |                     Space separated list of flagged tiles (enclosed in
             |                     quotes if more than one flag is specified) [default: none]
             |
             |Beamforming options (choose at least one):
             |  --fits             Export beamformed data in PSRFITS format
             |  --vdif             Export beamformed data in VDIF format
             |
             |Pointing options:
             |  --psrs <PSRS>...   Space separated list of pulsar Jnames (enclosed in
             |                     quotes if more than one pulsar is specified), e.g.,
             |                     "J1440-6344 J1453-6413 J1456-6843"
             |
             |Dedispersion and folding options:
             |  --nbin             Maximum phase bins to fold into [default: ${params.nbin}]
             |  --fine_chan        Number of fine channels per coarse channel (dspsr) [default: ${params.nchan}]
             |  --tint             Length of sub-integrations (dspsr) [default: ${params.tint} s]
             |  --nsub             Number of frequency sub-bands to use in search (prepfold) [default: ${params.nsub}]
             |  --npart            Number of sub-integrations to use in search (prepfold) [default: ${params.npart}]
             |
             |Optional arguments:
             |  --vcsbeam_version  The vcsbeam module version to use [default: ${params.vcsbeam_version}]
             |  -w                 The Nextflow work directory. Delete the directory once the
             |                     process is finished [default: ${workDir}]""".stripMargin()
    println(help)
    exit(0)
}

if ( params.fits != true && params.vdif != true ) {
    println('No file format selected.')
    exit(1)
}

include { beamform } from './modules/singlebeam_module'
include { beamform_mp } from './modules/multibeam_module'

workflow {
    if ( params.fits ) {
        Channel
            .from(params.psrs.split(' '))
            .collect()
            .set { psrs }
        beamform_mp(psrs)
    }
    if ( params.vdif ) {
        Channel
            .from(params.psrs.split(' '))
            .set { psrs }
        beamform(psrs)
    }
}