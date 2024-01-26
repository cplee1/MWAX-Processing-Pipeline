process PUBLISH_VCSBEAM_FILES {
    tag "${source}"

    input:
    val(source)
    val(source_dir)
    val(duration)
    path(files)

    script:
    """
    mv -t "${source_dir}/vdif_${duration}s" ${files}
    """
}
