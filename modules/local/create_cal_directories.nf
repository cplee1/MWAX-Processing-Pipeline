process CREATE_CAL_DIRECTORIES {
    input:
    val(calid)
    val(cal_dir)

    output:
    val(true)

    script:
    """
    obs_dir=${cal_dir}/${calid}

    if [[ ! -d \${obs_dir} ]]; then
        echo "Error: Cannot locate calibration directory \${obs_dir}."
        exit 1
    fi
    if [[ ! -d "\${obs_dir}/hyperdrive" ]]; then
        mkdir -p -m 771 "\${obs_dir}/hyperdrive"
    elif [[ -r "\${obs_dir}/hyperdrive/hyperdrive_solutions.bin" ]]; then
        archive="\${obs_dir}/hyperdrive/archived_\$(date +%s)"
        mkdir -p -m 771 \$archive
        mv \${obs_dir}/hyperdrive/*solutions* \$archive
    fi

    metafits=\$(find \${obs_dir}/*.metafits)

    if [[ \$(echo \$metafits | wc -l) -ne 1 ]]; then
        echo "Error: Unique metafits file not found."
        exit 1
    fi
    """
}
