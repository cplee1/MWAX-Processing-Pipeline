process VCSBEAM {
    label 'gpu'
    label 'vcsbeam'

    tag "${psr}"

    maxForks 3

    time { 1.hour * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'finish' }
    maxRetries 1

    input:
    val(psr)
    val(source_dir)
    val(data_dir)
    val(duration)
    val(begin)
    val(low_chan)
    val(obs_metafits)
    val(cal_metafits)
    val(cal_solution)
    val(flagged_tiles)
    val(pointings)

    output:
    path('*.{vdif,hdr}')

    script:
    """
    make_mwa_tied_array_beam -V
    echo "\$(date): Executing make_mwa_tied_array_beam."
    srun make_mwa_tied_array_beam \\
        -m ${obs_metafits} \\
        -b ${begin} \\
        -T ${duration} \\
        -f ${low_chan} \\
        -d ${data_dir} \\
        -P ${pointings} \\
        -F ${flagged_tiles} \\
        -c ${cal_metafits} \\
        -C ${cal_solution} \\
        -R NONE -U 0,0 -O -X --smart -v
    echo "\$(date): Finished executing make_mwa_tied_array_beam."
    """
}
