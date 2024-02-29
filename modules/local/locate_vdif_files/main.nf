process LOCATE_VDIF_FILES {
    tag "${source}"

    input:
    val(source)
    val(pointings_dir)
    val(duration)

    output:
    tuple val(source), path('*.{vdif,hdr}')

    script:
    """
    vdif_dir="${pointings_dir}/${source}/vdif_${duration}s"
    if [[ ! -d \$vdif_dir ]]; then
        echo "ERROR :: Cannot locate data directory: \${vdif_dir}"
        exit 1
    fi
    find \$vdif_dir -type f -name "*.vdif" -exec ln -s '{}' \\;
    find \$vdif_dir -type f -name "*[0-9].hdr" -exec ln -s '{}' \\;

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
