#!/bin/bash

# Beamform non-contiguous observations with VCSBeam

# The scripts assume a standard directory structure, as follows:
#
#   The base directory is ${root}/${obsid}
#   The metafits file of the VCS observation is kept in the base directory
#   Raw voltage files are kept in ${root}/${obsid}/combined
#   The calibration data are kept in ${root}/${obsid}/cal/${calid}
#   The calibration solutions are kept in ${root}/${obsid}/cal/${calid}/hyperdrive
#   The calibration solutions are named hyperdrive_solutions_ch${LOWCHAN}-${HIGHCHAN}.bin
#
# The flagged_tiles.txt and pointings.txt files should be placed in the current
# working directory. This is also where the output will be placed.

#===============================================================================
# Required inputs
#-------------------------------------------------------------------------------
# obs ID of the VCS observation
obsid=
# obs ID of the calibrator
calid=
# the starting GPS second of the observation
startgps=
# how many seconds to process
duration=
# an array of channel indices indicating the lowest channel of each band
lowchans=(  )
# an array of channel indices indicating the highest channel of each band
highchans=(  )
# the root directory of the downloaded data (backslashes must be escaped for sed)
root_dir="\/astro\/mwavcs\/${USER}\/vcs_downloads"
# location of the template scripts
script_dir=/astro/mwavcs/cplee/github/MWAX-Processing-Pipeline/example_scripts/picket_fence
#===============================================================================

if [[ ${#lowchans[@]} -ne ${#highchans[@]} ]]; then
    echo "Error: The channel arrays are not the same length. Exiting."
    exit 1
fi

# Create slurm scripts for each contiguous band, then submit the jobs
for ((band_idx=0; band_idx<${#lowchans[@]}; band_idx++)); do
    lowchan=${lowchans[band_idx]}
    highchan=${highchans[band_idx]}
    numchan=$((highchan - lowchan))
    beamform_script=beamform_ch${lowchan}-${highchan}.batch

    cp ${script_dir}/beamform_template.batch $beamform_script
    sed -i "s/OBSID/${obsid}/g" $beamform_script
    sed -i "s/CALID/${calid}/g" $beamform_script
    sed -i "s/LOWCHAN/${lowchan}/g" $beamform_script
    sed -i "s/HIGHCHAN/${highchan}/g" $beamform_script
    sed -i "s/NUMCHAN/${numchan}/g" $beamform_script
    sed -i "s/STARTGPS/${startgps}/g" $beamform_script
    sed -i "s/DURATION/${duration}/g" $beamform_script
    sed -i "s/ROOTDIR/${root_dir}/g" $beamform_script

    echo "sbatch $beamform_script"
    sbatch $beamform_script
done