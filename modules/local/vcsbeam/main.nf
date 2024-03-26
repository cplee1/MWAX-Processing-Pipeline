process VCSBEAM {
    label 'gpu'
    label 'vcsbeam'

    tag "${psr}"

    maxForks 3

    time 14.hour
    errorStrategy 'finish'

    // time { 8.hour * task.attempt }
    // errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'finish' }
    // maxRetries 2

    input:
    tuple val(psr), val(pointings)
    val(data_dir)
    val(duration)
    val(begin)
    val(low_chan)
    val(cal_files)
    val(flagged_tiles)

    output:
    tuple val(psr), path('*.{vdif,hdr}')

    script:
    """
    make_mwa_tied_array_beam -V
    echo "\$(date): Executing make_mwa_tied_array_beam."
    srun make_mwa_tied_array_beam \\
        -m ${cal_files.obs_meta} \\
        -b ${begin} \\
        -T ${duration} \\
        -f ${low_chan} \\
        -d ${data_dir} \\
        -P ${pointings} \\
        -F ${flagged_tiles} \\
        -c ${cal_files.cal_meta} \\
        -C ${cal_files.cal_sol} \\
        -R NONE -U 0,0 -O -X --smart -v
    echo "\$(date): Finished executing make_mwa_tied_array_beam."
    """
}
