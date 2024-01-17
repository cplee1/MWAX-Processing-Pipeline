process MOVE_DATA {
    tag "${obsid_to_move}"

    input:
    val(ready)
    val(base_dir)
    val(asvo_path)
    val(vcs_obsid)
    val(obsid_to_move)
    val(mode)

    output:
    val(true)

    script:
    if ( mode == 'vcs' ) {
    """
        if [[ ! -d ${base_dir} ]]; then
            echo "Error: VCS directory does not exist."
            exit 1
        fi

        # Create a directory to move files into
        mkdir -p -m 771 ${base_dir}/${obsid_to_move}/combined

        # Move data
        mv ${fpath}/*.sub ${base_dir}/${obsid_to_move}/combined
        mv ${fpath}/*.metafits ${base_dir}/${obsid_to_move}
        
        # Delete the job directory
        if [[ -d ${fpath} ]]; then
            if [[ -r ${fpath}/MWA_ASVO_README.md ]]; then
                rm ${fpath}/MWA_ASVO_README.md
            fi
            if [[ -z "\$(ls -A ${fpath})" ]]; then
                rm -r ${fpath}
            else
                echo "Job directory not empty: ${fpath}."
            fi
        fi
        """
    } else if ( mode == 'vis' ) {
        """
        if [[ ! -d ${base_dir} ]]; then
            echo "Error: VCS directory does not exist."
            exit 1
        fi

        # Create a directory to move files into
        mkdir -p -m 771 ${base_dir}/${vcs_obsid}/cal/${obsid_to_move}

        # Move data
        mv ${fpath}/* ${base_dir}/${vcs_obsid}/cal/${obsid_to_move}

        # Delete the job directory
        if [[ -d ${fpath} ]]; then
            if [[ -r ${fpath}/MWA_ASVO_README.md ]]; then
                rm ${fpath}/MWA_ASVO_README.md
            fi
            if [[ -z "\$(ls -A ${fpath})" ]]; then
                rm -r ${fpath}
            else
                echo "Job directory not empty: ${fpath}."
            fi
        fi
        """
    } else {
        System.err.println("ERROR: Invalid file transfer mode encountered: ${mode}")
    }
}
