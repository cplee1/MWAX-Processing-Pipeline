process VCSBEAM_MULTIPIXEL {
    label 'vcsbeam'

    maxForks 1
    time "${ params.vcsbeam_mp_min_walltime + 2 * (task.attempt - 1) }h"
    maxRetries 1
    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'finish' }

    input:
    tuple val(pointings), val(pairs), val(flagged_tiles)
    val(pointings_dir)
    val(data_dir)
    val(duration)
    val(begin)
    val(low_chan)
    val(cal_files)

    output:
    val(pairs)

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
        -R NONE -U 0,0 -O -X --smart -p
    echo "\$(date): Finished executing make_mwa_tied_array_beam."

    # Get list of sources
    eval "pulsars=(\$(cat ${pairs} | awk '{print \$1}'))"

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
