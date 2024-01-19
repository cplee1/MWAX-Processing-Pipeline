process DSPSR {
    label 'cpu'
    label 'psranalysis'
    label 'dspsr'

    tag "${psr}"

    time { 2.hour * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'ignore' }
    maxRetries 2

    input:
    val(psr)
    val(source_dir)
    val(duration)
    val(nbin)
    val(fine_chan)
    val(tint)
    path(vcsbeam_files)
    path(par_file)

    output:
    val(true)

    script:
    """
    find *.hdr | sort > headers.txt
    find *.vdif | sort > vdiffiles.txt

    if [[ -z \$(cat headers.txt) ]]; then
        echo "Error: No header files found."
        exit 1
    fi

    spin_freq=\$(grep F0 ${par_file} | awk '{print \$2}')
    if [[ -z \$spin_freq ]]; then
        echo "Error: Cannot locate spin frequency."
        exit 1
    fi
    
    # Bin number computations
    spin_period_ms=\$(echo "1000 / \$spin_freq" | bc -l)
    bin_time_res_ms=\$(echo "\$spin_period_ms/${nbin}" | bc -l)
    nq_time_res_ms=\$(echo "(${fine_chan}*${num_chan})/(1.28*10^6)*10^3" | bc -l)
    if (( \$(echo "\$bin_time_res_ms < \$nq_time_res_ms" | bc -l) )); then
        nbin=\$(echo "\$spin_period_ms/\$nq_time_res_ms" | bc)
    else
        nbin=${nbin}
    fi

    for datafile_hdr in \$(cat headers.txt); do
        if [ ! -s \$datafile_hdr ]; then
            echo "Error: Invalid hdr file \'\${datafile_hdr}\'. Skipping file."
        else
            new_datafile_hdr="\${datafile_hdr%%.hdr}_updated.hdr"
            cat \$datafile_hdr | tr -c -d '[:print:]\\n\\t' > \$new_datafile_hdr
            printf "NPOL 2\\n" >> \$new_datafile_hdr
            datafile_vdif=\${datafile_hdr%%.hdr}.vdif
            if [ ! -s \$datafile_vdif ]; then
                echo "Error: Invalid vdif file \'\${datafile_vdif}\'. Skipping file."
            else
                size_mb=4096
                outfile=\${datafile_hdr%.hdr}
                dspsr \\
                    -E ${par_file} \\
                    -b \$nbin \\
                    -U \$size_mb \\
                    -F ${fine_chan}:D \\
                    -L ${tint} -A \\
                    -O \$outfile \\
                    \$new_datafile_hdr
            fi
        fi
    done

    # Make a list of channel archives to delete
    find *.ar | xargs -n1 basename > channel_archives.txt

    # The name of the combined archive
    base_name=${psr}_bins\${nbin}_fchans${fine_chan}_tint${tint}

    # Stitch together channels and delete individual channel archives
    psradd -R -o \${base_name}.ar *.ar
    cat channel_archives.txt | xargs rm
    rm channel_archives.txt

    # Flag first time integration
    paz -s 0 -m \${base_name}.ar

    # Plotting
    pav -FTp -C -Dd -g \${base_name}_pulse_profile.png/png \${base_name}.ar
    pav -Tp -C -Gd -g \${base_name}_frequency_phase.png/png \${base_name}.ar
    pav -Fp -C -Yd -g \${base_name}_time_phase.png/png \${base_name}.ar

    dataproduct_dir=${source_dir}/vdif_${duration}s
    if [[ ! -d \${dataproduct_dir}/dspsr ]]; then
        mkdir -p -m 771 \${dataproduct_dir}/dspsr
    fi

    # Move files to publish directory
    mv *_updated.hdr \${dataproduct_dir}
    mv *.ar *.png \${dataproduct_dir}/dspsr
    cp -L -t \${dataproduct_dir}/dspsr *.par

    # If there are beamformed files already, we are re-folding, so skip this step
    old_files=\$(find \$dataproduct_dir -type f -name "*.vdif")
    if [[ -z \$old_files ]]; then
        # Copy VDIF/HDR files into the publish directory and delete from the work directory
        cat vdiffiles.txt | xargs -n1 cp -L -t \$dataproduct_dir
        cat vdiffiles.txt | xargs -n1 readlink -f | xargs -n1 rm
        cat headers.txt | xargs -n1 cp -L -t \$dataproduct_dir
        cat headers.txt | xargs -n1 readlink -f | xargs -n1 rm
    fi
    """
}
