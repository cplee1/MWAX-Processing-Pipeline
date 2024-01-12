# MWAX Processing Pipeline
Nextflow pipelines for processing VCS pulsar observations.

## Installation
These scripts are currently setup for Pawsey's Garrawarla cluster, but will
eventually be expanded for use on Setonix. I am working on setting up a module,
but in the meantime you can use the pipelines by cloning the repository
and then adding it to you PATH, e.g.

    export PATH=${PATH}:/scratch/mwavcs/${USER}/github/MWAX-Processing-Pipeline

This pipeline uses Nextflow to schedule jobs and manage intermediate data
products. To run the pipeline, you need to have the Nextflow binary installed
and in the PATH. On Garrawarla, the `mwa_search` module already has Nextflow
installed, so one way to setup the environment is:

    module use /pawsey/mwa/software/python3/modulefiles
    module load mwa_search

If the repository is in your PATH, then the scripts should be executable from
anywhere.

## General tips
All pipelines have a help menu available by setting the `--help` flag. The help
also includes common examples, but if you only want the examples then they are
available using the `--examples` flag.

## Downloading VCS data
The `mwax_download.nf` pipeline has two main functions:

* Download observations from the MWA ASVO
* Move the downloaded data into a standardised directory structure

Downloading is done using Giant Squid. It submits jobs to ASVO, then waits until
the files are downloaded and moves them into the standard directory structure.
If you prefer to use the ASVO Web Client, then you can skip the download step
and instead provide the ASVO job IDs.

## Calibrating VCS data
The `mwax_calibrate.nf` pipeline generates calibration solutions from MWA
imaging observations using Birli (to preprocess) and Hyperdrive (to calibrate).
The solutions are stored in both Hyperdrive FITS format and the legacy Offringa
format for compatibility with VCSBeam. Calibrator observations are specified as
"OBSID:SOURCE" pairs, where SOURCE is either a source from `source_lists.txt` or
any other string. The former will use a multi-component Gaussian model and the
latter will default to using the GLEAM-X Galactic Sky Map. In general, I
recommend trying the GLEAM-X source list first using, e.g., "OBSID:-" as the
calibrator input.

## Beamforming and folding on pulsars
The `mwax_beamform.nf` pipeline performs a variety of functions:

* Beamforming using VCSBeam
* Folding and parameter optimisation using `dspsr`/`pdmp` or `prepfold`
* Uploading the final data products to Acacia

The beamformer can output in PSRFITS format (Stokes intensities) or in VDIF
format (complex voltages). When outputting PSRFITS, the pipeline/beamformer
operated in "multi-pixel" mode, which submits one big beamforming job then
post-processes the output as normal (per pular). For VDIF output, the
pipeline/beamformer operates normally, where all pulsars are beamformed and
processed in separate jobs. If you want both PSRFITS and VDIF dataproducts,
the pipeline is able to run both workflows in parallel.
