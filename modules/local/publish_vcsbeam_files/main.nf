process PUBLISH_VCSBEAM_FILES {
    tag "${source}"

    input:
    val(source)
    val(pointings_dir)
    val(duration)
    path(files)

    script:
    """
    mv -t "${pointings_dir}/${source}/vdif_${duration}s" ${files}
    """
}
