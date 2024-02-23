process CREATE_DATA_DIRECTORIES {
    tag "${source}"

    input:
    val(do_fits)
    val(do_vdif)
    val(pointings_dir)
    val(source)
    val(duration)

    output:
    env(source_dir), emit: source_dir
    val(pointings_dir), emit: pointings_dir

    script:
    if ( do_fits && do_vdif ) {
        """
        source_dir="${pointings_dir}/${source}"

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
        source_dir="${pointings_dir}/${source}"

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
        source_dir="${pointings_dir}/${source}"

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
