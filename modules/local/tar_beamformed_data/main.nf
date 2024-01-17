process TAR_BEAMFORMED_DATA {
    label 'cpu'
    label 'tar'

    tag "${psr}"

    time 1.hour

    input:
    val(psr)
    path(vcsbeam_files)

    output:
    path('*.tar')

    script:
    """
    dir_name="${psr}"
    mkdir -p "\$dir_name"

    cp -t "\$dir_name" *.vdif
    cp -t "\$dir_name" *.hdr

    # Follow symlinks and archive
    tar -cvhf "\${PWD}/\${dir_name}.tar" "\$dir_name"

    # Follow links and delete vdif
    find \$PWD -mindepth 1 -maxdepth 1 -name "*.vdif" | xargs -n1 readlink -f | xargs -n1 rm
    """
}
