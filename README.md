# MWAX Processing Pipeline
Nextflow pipelines for processing VCS pulsar observations. These pipelines are 
more up to date but less comprehensive than mwa_search. The beamforming and
calibration scripts should in principle be compatible with both legacy and MWAX
VCS data, but as of yet have only been tested with MWAX data.

## Installation
These scripts are setup for use on Pawsey's Garrawarla cluster, but will
eventually be expanded for use on Setonix. I am working on setting up a module,
but in the meantime you can set up the pipelines by cloning the repository
and then adding the `nextflow` directory to you PATH, e.g.

    export PATH=${PATH}:/astro/mwavcs/${USER}/github/MWAX-Processing-Pipeline/nextflow

## Preparing VCS data
After you have downloaded the VCS observation and a calibrator observation from
the ASVO, you can move the files into the standard directory structure using
`mwax_download.nf`. Available options and an example are given in the help menu:
    
    mwax_download.nf --help

## Calibrating VCS data
The calibration pipeline uses Birli to preprocess the FITS data, then Hyperdrive
to perform direction-independent amplitude/phase calibration. See the help menu
for available options and typical examples:

    mwax_calibrate.nf --help

## Beamforming and folding on pulsars
The beamforming pipeline uses VCSBeam, which can output baseband complex 
voltages in VDIF file format or detected timeseries in PSRFITS format. If
PSRFITS format is selected, then the multipixel beamformer will be used. If
VDIF format is selected, then the pulsars will be beamformed separately.
Dedispersion and folding is done with `prepfold` for PSRFITS data, and `dspsr`
for VDIF data. Detection optimisation is done with `prepfold` and `pdmp`.
See the help menu for available options and typical examples:

    mwax_beamform.nf --help

## Potential future improvements

`mwax_download.nf`

    - Download files with [giant-squid](https://github.com/MWATelescope/giant-squid)

`mwax_calibrate.nf`

    - Calibrate picket fence data

`mwax_beamform.nf`

    - Take pointings as an input

    - Skip the folding stage and just publish the beamformed data

    - Beamform picket fence data