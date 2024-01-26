process CREATE_DATA_DIRECTORIES {
    tag "${source}"

    input:
    val(ready)
    val(do_fits)
    val(do_vdif)
    val(base_dir)
    val(obsid)
    val(source)
    val(duration)

    output:
    env(data_dir), emit: data_dir
    env(pointings_dir), emit: pointings_dir
    env(source_dir), emit: source_dir

    script:
    if ( do_fits && do_vdif ) {
        """
        if [[ ! -d ${base_dir} ]]; then
            echo "ERROR :: Base directory does not exist: ${base_dir}"
            exit 1
        fi

        if [[ ! -d ${base_dir}/${obsid} ]]; then
            echo "ERROR :: Observation directory does not exist: ${base_dir}/${obsid}"
            exit 1
        fi

        if [[ ! -d ${base_dir}/${obsid}/combined ]]; then
            echo "ERROR :: Data directory does not exist: ${base_dir}/${obsid}/combined"
            exit 1
        fi

        data_dir="${base_dir}/${obsid}/combined"
        pointings_dir="${base_dir}/${obsid}/pointings"
        source_dir="\${pointings_dir}/${source}"

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
        """
    } else if ( do_fits ) {
        """
        if [[ ! -d ${base_dir} ]]; then
            echo "ERROR :: Base directory does not exist: ${base_dir}"
            exit 1
        fi

        if [[ ! -d ${base_dir}/${obsid} ]]; then
            echo "ERROR :: Observation directory does not exist: ${base_dir}/${obsid}"
            exit 1
        fi

        if [[ ! -d ${base_dir}/${obsid}/combined ]]; then
            echo "ERROR :: Data directory does not exist: ${base_dir}/${obsid}/combined"
            exit 1
        fi

        data_dir="${base_dir}/${obsid}/combined"
        pointings_dir="${base_dir}/${obsid}/pointings"
        source_dir="\${pointings_dir}/${source}"

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
        """
    } else if ( do_vdif ) {
        """
        if [[ ! -d ${base_dir} ]]; then
            echo "ERROR :: Base directory does not exist: ${base_dir}"
            exit 1
        fi

        if [[ ! -d ${base_dir}/${obsid} ]]; then
            echo "ERROR :: Observation directory does not exist: ${base_dir}/${obsid}"
            exit 1
        fi

        if [[ ! -d ${base_dir}/${obsid}/combined ]]; then
            echo "ERROR :: Data directory does not exist: ${base_dir}/${obsid}/combined"
            exit 1
        fi

        data_dir="${base_dir}/${obsid}/combined"
        pointings_dir="${base_dir}/${obsid}/pointings"
        source_dir="\${pointings_dir}/${source}"

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
        """
    }
}
