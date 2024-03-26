process CREATE_DIR_STRUCTURE {
    input:
    val(ready)
    val(vcs_dir)
    val(obsid)
    val(duration)
    val(do_fits)
    val(do_vdif)
    val(sources)

    output:
    tuple env(data_dir), env(pointings_dir)

    script:
    """
    if [[ ! -d ${vcs_dir} ]]; then
        echo "ERROR :: Specified VCS directory does not exist: ${vcs_dir}"
        exit 1
    fi

    if [[ ! -d ${vcs_dir}/${obsid} ]]; then
        echo "ERROR :: Observation directory does not exist: ${vcs_dir}/${obsid}"
        exit 1
    fi

    if [[ ! -d ${vcs_dir}/${obsid}/combined ]]; then
        echo "ERROR :: Data directory does not exist: ${vcs_dir}/${obsid}/combined"
        exit 1
    fi

    data_dir="${vcs_dir}/${obsid}/combined"
    pointings_dir="${vcs_dir}/${obsid}/pointings"

    for source_name in ${sources.join(' ')}; do
        source_dir="\${pointings_dir}/\${source_name}"

        if [[ "${do_fits}" == "true" ]]; then
            psrfits_dir="\${source_dir}/psrfits_${duration}s"
            if [[ ! -d \$psrfits_dir ]]; then
                mkdir -p -m 771 \$psrfits_dir
            fi
            old_psrfits_files=\$(find \$psrfits_dir -type f)
            if [[ -n \$old_psrfits_files ]]; then
                psrfits_archive="\${psrfits_dir}/beamformed_data_archived_\$(date +%s)"
                mkdir -p -m 771 \$psrfits_archive
                echo \$old_psrfits_files | xargs -n1 mv -t \$psrfits_archive
            fi
        fi

        if [[ "${do_vdif}" == "true" ]]; then
            vdif_dir="\${source_dir}/vdif_${duration}s"
            if [[ ! -d \$vdif_dir ]]; then
                mkdir -p -m 771 \$vdif_dir
            fi
            old_vdif_files=\$(find \$vdif_dir -type f)
            if [[ -n \$old_vdif_files ]]; then
                vdif_archive="\${vdif_dir}/beamformed_data_archived_\$(date +%s)"
                mkdir -p -m 771 \$vdif_archive
                echo \$old_vdif_files | xargs -n1 mv -t \$vdif_archive
            fi
        fi
    done
    """
}
