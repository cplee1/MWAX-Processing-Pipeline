process CREATE_CAL_DIRECTORIES {
    input:
    tuple val(calid), val(cal_dir), val(source), val(flagged_tiles), val(flagged_fine_chans)

    output:
    tuple val(calid), val(cal_dir), val(source), val(flagged_tiles), val(flagged_fine_chans), env(metafits)

    script:
    """
    if [[ ! -d ${cal_dir} ]]; then
        echo "Error: Cannot locate calibration directory ${cal_dir}."
        exit 1
    fi
    if [[ ! -d ${cal_dir}/hyperdrive ]]; then
        mkdir -p -m 771 ${cal_dir}/hyperdrive
    elif [[ -r ${cal_dir}/hyperdrive/hyperdrive_solutions.bin ]]; then
        archive="${cal_dir}/hyperdrive/archived_\$(date +%s)"
        mkdir -p -m 771 \$archive
        mv ${cal_dir}/hyperdrive/*solutions* \$archive
    fi

    metafits=\$(find ${cal_dir}/*.metafits)

    if [[ \$(echo \$metafits | wc -l) -ne 1 ]]; then
        echo "Error: Unique metafits file not found."
        exit 1
    fi
    """
}
