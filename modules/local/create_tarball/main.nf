process CREATE_TARBALL {
    label 'cpu'
    label 'tar'

    tag "${label}"

    time 1.hour

    input:
    tuple val(label), path(files)

    output:
    path("${label}.tar")

    script:
    """
    tar -cvhf "${label}.tar" ${files}
    """
}
