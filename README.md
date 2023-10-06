# MWAX Processing Pipeline
Nextflow workflows for processing VCS pulsar observations. These workflows are
yet to be tested on legacy observations, but should in principle be compatible.

## Installation
These scripts are setup for use on Pawsey's Garrawarla cluster, but will
eventually be expanded for use on Setonix. I am working on setting up a module,
but in the meantime you can set up the pipelines by cloning the repository
and then adding the `nextflow` directory to you PATH, e.g.

    export PATH=${PATH}:/astro/mwavcs/${USER}/github/MWAX-Processing-Pipeline/nextflow

## Downloading VCS data
The download pipeline uses Giant Squid to submit jobs to ASVO, then waits until
the files are downloaded and moves them into the standard directory structure.
If the files are already downloaded, then you can skip the download step and
instead provide the job IDs. This will move the files from the ASVO directory
into the VCS directory. Available options and an example are given in the help menu:
    
    mwax_download.nf --help

## Calibrating VCS data
The calibration pipeline uses Birli to preprocess and then Hyperdrive to
perform direction-independent amplitude/phase calibration an all tiles.
Calibrator observations are specified as OBSID:SOURCE pairs. If the source is
in the lookup table (source_lists.txt) then the specific source model will be
used, otherwise the GLEAM catalogue will be used. See the help menu for
available options and typical examples:

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

* Calibrate and beamform picket fence data
* Beamform on all pulsars in an observation