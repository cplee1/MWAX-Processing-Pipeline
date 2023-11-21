#!/bin/bash

# Calibrate non-contiguous observations with Birli and Hyperdrive

# Run this script in a directory where the FITS files and METAFITS file
# are located in the parent directory (../)

#===============================================================================
# Required inputs
#-------------------------------------------------------------------------------
# location of the template scripts
script_dir=/astro/mwavcs/cplee/github/MWAX-Processing-Pipeline/example_scripts
#===============================================================================

# Only run Birli if UVFITS files cannot be found
if [[ -z $(find .. -maxdepth 1 -name '*_birli_ch*.uvfits') ]]; then
    cp ${script_dir}/birli.batch .
    echo "sbatch birli.batch"
    sbatch birli.batch
fi

# Create slurm scripts for each contiguous band, then submit the jobs
uvfits_files=$(find .. -maxdepth 1 -name '*_birli_ch*.uvfits')
for uvfits_file in $uvfits_files; do
    lowchan=$(echo "$uvfits_file" | grep -oP 'ch(\d{2,3})' | sed 's/ch//')
    highchan=$(echo "$uvfits_file" | grep -oP '\-(\d{2,3})' | sed 's/-//')
    calibrate_script=calibrate_ch${lowchan}-${highchan}.batch

    cp ${script_dir}/picket_fence/calibrate_template.batch $calibrate_script
    sed -i "s/LOWCHAN/${lowchan}/g" $calibrate_script
    sed -i "s/HIGHCHAN/${highchan}/g" $calibrate_script
    sed -i "s/FITSFILELIST/${filelist}/g" $calibrate_script

    echo "sbatch $calibrate_script"
    sbatch $calibrate_script
done