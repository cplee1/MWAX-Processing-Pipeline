# MWAX Processing Pipeline
Nextflow pipelines for processing VCS pulsar observations.

## Installation
These scripts are currently setup for Pawsey's Garrawarla cluster, but can be
adapted for use on other clusters by providing a custom config file. To use the
pipeline, clone the repository and add it to your PATH, e.g.

    export PATH=${PATH}:/scratch/mwavcs/${USER}/github/MWAX-Processing-Pipeline

This pipeline uses Nextflow to schedule jobs and manage intermediate data
products. To run the pipeline, you need to have the Nextflow binary installed
and in the PATH. To do this, run

    wget -qO- https://get.nextflow.io | bash

and place the binary into a directory within your PATH. Then run

    module load java/17

You should now be able to run the scripts from anywhere.

## General tips
The pipelines have help available by setting the `--help` flag. The help also
includes common examples, but if you only want the examples then they are
available using the `--examples` flag.

## Downloading, beamforming, and folding pulsar data
The `mwax_beamform.nf` pipeline performs a variety of functions:

* Downloads observations from the MWA ASVO
* Moves the downloaded data into a standardised directory structure
* Beamforms using VCSBeam
* Folds and optimises using `dspsr`/`pdmp` or `prepfold`
* Uploads the final data products to Acacia

Downloading is done using Giant Squid. It submits jobs to ASVO, then waits until
the files are downloaded and moves them into the standard directory structure.
If you prefer to use the ASVO Web Client, then you can skip the download step
and instead provide the ASVO job IDs. Or if you already have the data downloaded
to the correct directories, you can skip this stage all together.

To beamform with this pipeline, the calibration solutions should either be in
the `hyperdrive` directory or the calibration solution repository.

The beamformer can output in PSRFITS format (Stokes intensities) or in VDIF
format (complex voltages). When outputting PSRFITS, the pipeline/beamformer
operated in "multi-pixel" mode, which submits one big beamforming job then
post-processes the output as normal (per pular). For VDIF output, the
pipeline/beamformer operates normally, where all pulsars are beamformed and
processed in separate jobs. If you want both PSRFITS and VDIF dataproducts,
the pipeline is able to run both workflows in parallel.

## Calibrating VCS data
The `mwax_calibrate.nf` pipeline generates calibration solutions from MWA
imaging observations using Birli (to preprocess) and Hyperdrive (to calibrate).
The solutions are stored in both Hyperdrive FITS format and the legacy AO
format for compatibility with VCSBeam. Calibrator observations are specified as
"OBSID:SOURCE" pairs, where SOURCE is either a source from `source_lists.txt` or
any other string. The former will use a multi-component Gaussian model and the
latter will default to using the GLEAM-X Galactic Sky Map. In general, I
recommend trying the GLEAM-X source list first using, e.g., "OBSID:-" as the
calibrator input.
