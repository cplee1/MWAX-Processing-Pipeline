process HYPERDRIVE {
    label 'gpu'
    label 'hyperdrive'

    time { 30.minute * task.attempt }

    maxForks 10

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    maxRetries 1
    publishDir "${cal_dir}/hyperdrive", mode: 'copy'

    input:
    tuple val(calid), val(cal_dir), val(flagged_tiles), val(flagged_fine_chans), val(metafits), path(srclist)

    output:
    tuple val(calid), val(cal_dir), path("hyperdrive_solutions.fits"), path("hyperdrive_solutions.bin"), path("*.png")
    
    script:
    """
    hyperdrive -V

    if [[ ! -r ${cal_dir}/${calid}_birli.uvfits ]]; then
        echo "Error: readable UVFITS file not found."
        exit 1
    fi

    tile_flag_opt=''
    if [[ ! -z '${flagged_tiles}' ]]; then
        tile_flag_opt='--tile-flags ${flagged_tiles}'
    fi
    chan_flag_opt=''
    if [[ ! -z '${flagged_fine_chans}' ]]; then
        chan_flag_opt='--fine-chan-flags-per-coarse-chan ${flagged_fine_chans}'
    fi

    # Perform DI calibration
    hyperdrive di-calibrate \\
        --source-list ${srclist} \\
        --data ${cal_dir}/${calid}_birli.uvfits ${metafits} \\
        \$tile_flag_opt \$chan_flag_opt

    # Plot the amplitudes and phases of the solutions
    hyperdrive solutions-plot \\
        --metafits ${metafits} \\
        hyperdrive_solutions.fits

    # Convert to Offringa format for VCSBeam
    hyperdrive solutions-convert \\
        --metafits ${metafits} \\
        hyperdrive_solutions.fits \\
        hyperdrive_solutions.bin
    """
}
