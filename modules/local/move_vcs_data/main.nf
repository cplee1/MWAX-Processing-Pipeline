process MOVE_VCS_DATA {
    tag "${obsid_to_move}"

    input:
    tuple val(job_id), val(dl_path), val(job_size)
    val(format)
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

    # Move the data
    if grep -q "combined" "${format}"; then
        mkdir -p -m 771 ${vcs_dir}/${obsid}/combined
        if [ -e \$(find ${dl_path} -type f -name *.sub | head -n1) ]; then
            mv ${dl_path}/*.sub ${vcs_dir}/${obsid}/combined
        else
            echo "Error: No data files found."
            exit 1
        fi
    elif grep -q "raw" "${format}"; then
        mkdir -p -m 771 ${vcs_dir}/${obsid}/raw
        if [ -e \$(find ${dl_path} -type f -name *.dat | head -n1) ]; then
            mv ${dl_path}/*.dat ${vcs_dir}/${obsid}/raw
        else
            echo "Error: No data files found."
            exit 1
        fi
    else
        echo "Error: Invalid data format."
        exit 1
    fi

    # Copy the metafits
    if [ -e ${dl_path}/${obsid}.metafits && ! -e ${vcs_dir}/${obsid}/${obsid}.metafits ]; then
        cp ${dl_path}/${obsid}.metafits ${vcs_dir}/${obsid}
    fi
    if [ -e ${dl_path}/${obsid}_metafits_ppds.fits && ! -e ${vcs_dir}/${obsid}/${obsid}_metafits_ppds.fits ]; then
        cp ${dl_path}/${obsid}_metafits_ppds.fits ${vcs_dir}/${obsid}
    fi
    """
}
