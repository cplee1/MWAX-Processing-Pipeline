process PREPFOLD {
    label 'cpu'
    label 'psrsearch'
    label 'prepfold'

    tag "${psr}"

    time 4.hour

    errorStrategy { task.attempt == 1 ? 'retry' : 'ignore' }
    maxRetries 1

    input:
    tuple val(psr), path(par_file), path(vcsbeam_files)
    val(pointings_dir)
    val(duration)
    val(num_chan)
    val(nbin)
    val(nsub)
    val(npart)
    val(nosearch)

    script:
    """
    find *.fits | sort > fitsfiles.txt

    bin_flag=""
    if [[ ! -z \$(grep BINARY ${par_file}) ]]; then
        bin_flag="-bin"
    fi

    nosearch_flag=""
    if [[ "${nosearch}" == "true" ]]; then
        nosearch_flag="-nosearch"
    fi

    par_input=""
    if [[ ${task.attempt} == 1 ]]; then
        # On first attempt, try the par file
        if [[ \$(cat ${par_file} | grep BINARY | awk '{print \$2}') == 'T2' ]]; then
            echo "Binary model T2 not accepted by TEMPO."
            # Default to PRESTO ephemeris
            par_input="-psr ${psr}"
        else
            par_input="-par ${par_file}"
        fi
    else
        # Otherwise, try the inbuilt ephermeris in PRESTO
        par_input="-psr ${psr}"
    fi

    spin_freq=\$(grep F0 ${par_file} | awk '{print \$2}')
    if [[ -z \$spin_freq ]]; then
        echo "Error: Cannot locate spin frequency."
        exit 1
    fi
    
    # Bin number computations
    spin_period_ms=\$(echo "1000 / \$spin_freq" | bc -l)
    bin_time_res_ms=\$(echo "\$spin_period_ms/${nbin}" | bc -l)
    nq_time_res_ms=\$(echo "${nsub}/(1.28*10^6)*10^3" | bc -l)
    if (( \$(echo "\$bin_time_res_ms < \$nq_time_res_ms" | bc -l) )); then
        nbin=\$(echo "\$spin_period_ms/\$nq_time_res_ms" | bc)
    else
        nbin=${nbin}
    fi

    # Compute the total number of channels
    nsub_total=\$(printf "%.0f" \$(echo "scale=0; ${nsub}*${num_chan}" | bc -l))

    prepfold \\
        -ncpus ${task.cpus} \\
        \$par_input \\
        -noxwin \\
        -noclip \\
        -noscales \\
        -nooffsets \\
        -n \$nbin \\
        -nsub \$nsub_total \\
        -npart ${npart} \\
        \$bin_flag \\
        \$nosearch_flag \\
        \$(cat fitsfiles.txt)

    dataproduct_dir=${pointings_dir}/${source}/psrfits_${duration}s
    if [[ ! -d \${dataproduct_dir}/prepfold ]]; then
        mkdir -p -m 771 \${dataproduct_dir}/prepfold
    fi

    # Move files to publish directory
    mv *pfd* \${dataproduct_dir}/prepfold
    cp -L -t \${dataproduct_dir}/prepfold *.par

    # If there are beamformed files already, we are re-folding, so skip this step
    old_files=\$(find \$dataproduct_dir -type f -name "*.fits")
    if [[ -z \$old_files ]]; then
        # Copy FITS files into the publish directory and delete from the work directory
        cat fitsfiles.txt | xargs -n1 cp -L -t \$dataproduct_dir
        cat fitsfiles.txt | xargs -n1 readlink -f | xargs -n1 rm
    fi
    """
}
