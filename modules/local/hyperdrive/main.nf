process HYPERDRIVE {
    label 'gpu'
    label 'hyperdrive'

    time { 30.minute * task.attempt }

    maxForks 10

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    maxRetries 1
    publishDir "${cal_dir}/${calid}/hyperdrive", mode: 'copy'

    input:
    val(calid)
    val(cal_dir)
    val(flagged_tiles)
    val(flagged_fine_chans)
    path(srclist)

    output:
    path("hyperdrive_solutions.fits"), emit: fits_sol
    path("hyperdrive_solutions.bin"), emit: ao_sol
    path("*.png"), emit: plots
    
    script:
    """
    obs_dir=${cal_dir}/${calid}

    if [[ ! -r "\${obs_dir}/${calid}_birli.uvfits" ]]; then
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
    hyperdrive -V
    hyperdrive di-calibrate \\
        --source-list ${srclist} \\
        --data "\${obs_dir}/${calid}_birli.uvfits" \${obs_dir}/${calid}.metafits \\
        \$tile_flag_opt \$chan_flag_opt

    # Plot the amplitudes and phases of the solutions
    hyperdrive solutions-plot \\
        --metafits \${obs_dir}/${calid}.metafits \\
        hyperdrive_solutions.fits

    # Convert to Offringa format for VCSBeam
    hyperdrive solutions-convert \\
        --metafits \${obs_dir}/${calid}.metafits \\
        hyperdrive_solutions.fits \\
        hyperdrive_solutions.bin
    """
}
