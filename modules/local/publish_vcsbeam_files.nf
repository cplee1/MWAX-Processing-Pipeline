process PUBLISH_VCSBEAM_FILES {
    tag "${source}"

    input:
    tuple val(source), path(files)
    val(pointings_dir)
    val(duration)

    script:
    """
    mv -t "${pointings_dir}/${source}/vdif_${duration}s" ${files}
    """
}
