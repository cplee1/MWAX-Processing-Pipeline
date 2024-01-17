process LOCATE_VDIF_FILES {
    tag "${source}"

    input:
    val(source)
    val(source_dir)
    val(duration)

    output:
    path('*.{vdif,hdr}')

    script:
    """
    vdif_dir="${source_dir}/vdif_${duration}s"
    if [[ ! -d \$psr_dir ]]; then
        echo "ERROR :: Cannot locate data directory: \${vdif_dir}"
        exit 1
    fi
    find \$vdif_dir -type f -name "*.vdif" -exec ln -s '{}' \\;
    find \$vdif_dir -type f -name "*.hdr" -exec ln -s '{}' \\;

    if [[ -d \${vdif_dir}/dspsr ]]; then
        old_dspsr_files=\$(find \${vdif_dir}/dspsr -type f)
        if [[ -n \$old_dspsr_files ]]; then
            archive="\${vdif_dir}/dspsr_archived_\$(date +%s)"
            mkdir -p -m 771 \$archive
            echo \$old_dspsr_files | xargs -n1 mv -t \$archive
        fi
    fi
    """
}
