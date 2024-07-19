process MOVE_VCS_DATA {
    tag "${obsid}"

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
        data_dir="${vcs_dir}/${obsid}/combined"
        mkdir -p -m 771 \$data_dir
        if [ ! -z \$(find ${dl_path} -type f -name *.sub | head -n1) ]; then
            # Move MWAX sub files
            mv ${dl_path}/*.sub \$data_dir
        fi
        if [ ! -z \$(find ${dl_path} -type f -name *.tar | head -n1) ]; then
            # Move legacy combined tar files
            mv ${dl_path}/*.tar \$data_dir
        fi
        if [ ! -z \$(find ${dl_path} -type f -name *_ics.dat | head -n1) ]; then
            # Move legacy ics dat files
            mv ${dl_path}/*_ics.dat \$data_dir
        fi
        if [ -z \$(find \$data_dir -type f | head -n1) ]; then
            echo "Error: No data files were moved."
        fi
    elif grep -q "raw" "${format}"; then
        data_dir="${vcs_dir}/${obsid}/raw"
        mkdir -p -m 771 \$data_dir
        if [ ! -z \$(find ${dl_path} -type f -name *.dat | head -n1) ]; then
            mv ${dl_path}/*.dat \$data_dir
        else
            echo "Error: No data files were moved."
        fi
    else
        echo "Error: Invalid data format."
        exit 1
    fi

    # Copy the metafits
    if [[ -r ${dl_path}/${obsid}.metafits && ! -r ${vcs_dir}/${obsid}/${obsid}.metafits ]]; then
        cp ${dl_path}/${obsid}.metafits ${vcs_dir}/${obsid}
    fi
    if [[ -r ${dl_path}/${obsid}_metafits_ppds.fits && ! -r ${vcs_dir}/${obsid}_metafits_ppds.fits ]]; then
        cp ${dl_path}/${obsid}_metafits_ppds.fits ${vcs_dir}/${obsid}
    fi
    """
}
