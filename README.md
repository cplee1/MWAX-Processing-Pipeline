# MWAX-Processing-Pipeline
A basic Nextflow pipeline for processing MWAX VCS data.

## Downloading VCS data
The following command will move files from the ASVO directory to the VCS directory.
Inputs in square brackets are optional. Default options are given in nextflow.config.
```
nextflow run mwax_download.nf --asvo_id_obs ASVOID --asvo_id_cal ASVOID1,[ASVOID2,...] [--asvo_dir /path/to/asvo/dir] [--vcs_dir /path/to/vcs/dir]
```
Remember to put a trailing comma after the calibrator ASVO ID if you only have one calibrator ID. e.g.
```
nextflow run mwax_download.nf --asvo_id_obs 661635 --asvo_id_cals 661634,
```

## Calibrating VCS data
The pipeline uses Birli to average in time and frequency, then Hyperdrive to perform direction independent amplitude/phase calibration.
Inputs in square brackets are optional. Default options are given in nextflow.config.
If the source is not listed in source_lists.txt, then it will default to the Gleam-X source catalogue.
```
nextflow run mwax_calibrate.nf --obsid OBSID --calibrators CALID1:SOURCE1,[CALID2:SOURCE2,...] [--flagged_tiles TILE1,[TILE2,...]] [--flag_edge_chans CHANS] [--force_birli]
```
Remember to put a trailing comma after the calibrator if you only have one.
For an initial calibration, no tiles flags will be added. e.g.

```
nextflow run mwax_calibrate.nf --obsid 1373387040 --calibrators 1373386920:HydA,
```
If there are bad tiles, add the tile IDs to the flags. If there is bad fine channels at the edge of each coase channel, add the edge channel flag and tell the pipeline to re-make the uvfits file with --force_birli. e.g.
```
nextflow run mwax_calibrate.nf --obsid 1373387040 --calibrators 1373386920:HydA, --flagged_tiles 52,55,135 --flag_edge_chans 1 --force_birli
```

## Beamforming on pulsars
Beamforming is done using VCSBeam, which is capable of outputing baseband voltages (VDIF file format) or detected timeseries (PSRFITS format).
Currently the main branch of this repository is setup to use the multipixel beamformer, which only works for PSRFITS format.
However, if only a single pulsar is given, the pipeline should work for VDIF as well.

To beamform with PSRFITS format, you must include the option --fits.

Inputs in square brackets are optional. Default options are given in nextflow.config (default duration is 592s and lowest channel is 109).
```
nextflow run mwax_beamform.nf --obsid OBSID --calid CALID --startgps STARTGPS --psrs PSR1,[PSR2,...] [--duration DURATION] [--low_chan LOWEST_CHAN] [--flagged_tiles TILE1,[TILE2,...]] [--fits]
```
For example:
```
nextflow run mwax_beamform.nf --obsid 1373387040 --calid 1373386920 --startgps 1373389144 --psrs J1911-1114,J1918-0642,2010-1323 --flagged_tiles 52,55,135 --fits
```
