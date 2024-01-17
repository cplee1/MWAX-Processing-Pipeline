process LOCATE_PSRFITS_FILES {
    tag "${source}"

    input:
    val(ready)
    val(source)
    val(source_dir)
    val(duration)

    output:
    path('*.fits')

    script:
    """
    psrfits_dir="${source_dir}/psrfits_${duration}s"
    if [[ ! -d \$psrfits_dir ]]; then
        echo "Error: Cannot locate data directory."
        exit 1
    fi
    find \$psrfits_dir -type f -name "*.fits" -exec ln -s '{}' \\;

    if [[ -d \${psrfits_dir}/prepfold ]]; then
        old_prepfold_files=\$(find \${psrfits_dir}/prepfold -type f)
        if [[ -n \$old_prepfold_files ]]; then
            archive="\${psrfits_dir}/prepfold_archived_\$(date +%s)"
            mkdir -p -m 771 \$archive
            echo \$old_prepfold_files | xargs -n1 mv -t \$archive
        fi
    fi
    """
}
