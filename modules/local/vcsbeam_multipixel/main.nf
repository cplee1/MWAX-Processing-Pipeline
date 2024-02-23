process VCSBEAM_MULTIPIXEL {
    label 'gpu'
    label 'vcsbeam'

    maxForks 3

    time { 8.hour * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'finish' }
    maxRetries 1

    input:
    val(sources)
    val(pointings_dir)
    val(data_dir)
    val(duration)
    val(begin)
    val(low_chan)
    val(obs_metafits)
    val(cal_metafits)
    val(cal_solution)
    val(flagged_tiles)
    val(pointings)
    val(pairs)

    output:
    val(true)

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
        -R NONE -U 0,0 -O -X --smart -p
    echo "\$(date): Finished executing make_mwa_tied_array_beam."

    # Turn the Nextflow list into a Bash array
    eval "pulsars=(\$(echo ${sources} | sed 's/\\[//;s/\\]//;s/,/ /g'))"

    # Organise files by pointing using the specified globs
    for (( i=0; i<\${#pulsars[@]}; i++ )); do
        pointing_glob=\$(grep "\${pulsars[i]}" ${pairs} | awk '{print \$2}')
        if [[ -z \$pointing_glob ]]; then
            echo "Error: Cannot find pointing for pulsar \${pulsars[i]}."
            exit 1
        fi
        find . -type f -name "\$pointing_glob" -exec mv -t "${pointings_dir}/\${pulsars[i]}/psrfits_${duration}s" '{}' \\;
    done
    """
}
