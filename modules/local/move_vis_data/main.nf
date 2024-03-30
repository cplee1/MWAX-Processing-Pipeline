process MOVE_VIS_DATA {
    tag "${obsid_to_move}"

    input:
    tuple val(job_id), val(dl_path), val(job_size)
    val(obsid)
    val(vcs_dir)

    output:
    val(true)

    script:
    """
    if [[ ! -d ${vcs_dir} ]]; then
        echo "Error: VCS directory does not exist."
        exit 1
    fi

    if [[ ! -d ${dl_path} ]]; then
        echo "Error: Download directory does not exist."
        exit 1
    fi

    # Get the obsid
    cal_obsid=\$(find ${dl_path} -name "*.metafits" | xargs -n1 basename -s ".metafits")

    if [[ \${#obsid} != 10 ]]; then
        echo "Error: The inferred obs ID is not valid."
        exit 1
    fi

    # Create a directory to move files into
    mkdir -p -m 771 ${vcs_dir}/${obsid}/cal/\${cal_obsid}

    # Move data
    mv ${dl_path}/* ${vcs_dir}/${obsid}/cal/\${cal_obsid}

    # Delete the job directory
    if [[ -r ${dl_path}/MWA_ASVO_README.md ]]; then
        rm ${dl_path}/MWA_ASVO_README.md
    fi
    if [[ -z "\$(ls -A ${dl_path})" ]]; then
        rmdir ${dl_path}
    else
        echo "Job directory not empty: ${dl_path}"
    fi
    """
}
