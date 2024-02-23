process PDMP {
    label 'cpu'
    label 'psranalysis'

    tag "${psr}"

    time { 2.hour * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'ignore' }
    maxRetries 1

    publishDir "${pointings_dir}/${psr}/vdif_${duration}s/dspsr/pdmp", mode: 'move'

    input:
    val(ready)
    val(psr)
    val(pointings_dir)
    val(duration)
    val(pdmp_mc)
    val(pdmp_ms)

    output:
    path('*.png')
    path('pdmp*')
    path('*bestP0DM')

    script:
    """
    base_dir="${pointings_dir}/${psr}/vdif_${duration}s/dspsr"
    find \$base_dir -type f -name "*.ar" -exec ln -s '{}' \\;
    ar_file=\$(find *.ar)
    if [[ \$(echo \$ar_file | wc -l) -gt 1 ]]; then
        echo "Error: More than one archive file found."
    fi

    nchan=\$(psredit -Qc nchan \$ar_file | awk '{print \$2}')
    nsubint=\$(psredit -Qc nsubint \$ar_file | awk '{print \$2}')

    mc_flag=""
    if [ ${pdmp_mc} -lt \$nchan -a \$((\$nchan % ${pdmp_mc})) -eq 0 ]; then
        mc_flag="-mc ${pdmp_mc}"
    fi

    ms_flag=""
    if [ ${pdmp_ms} -lt \$nsubint -a \$((\$nsubint % ${pdmp_ms})) -eq 0 ]; then
        ms_flag="-ms ${pdmp_ms}"
    fi

    pdmp \\
        \$mc_flag \\
        \$ms_flag \\
        -g \${ar_file%.ar}_pdmp.png/png \\
        \${ar_file} \\
        | tee pdmp.log

    # Update the period and DM
    best_p0_ms=\$(grep "Best TC Period" pdmp.log | awk '{print \$6}')
    best_p0_s=\$(printf "%.10f" \$(echo "scale=10; \$best_p0_ms / 1000" | bc))
    best_DM=\$(grep "Best DM" pdmp.log | awk '{print \$4}')
    pam -e bestP0DM --period \$best_p0_s -d \$best_DM \${ar_file}

    # Create the publish directory
    mkdir -p -m 771 \${base_dir}/pdmp
    """
}
