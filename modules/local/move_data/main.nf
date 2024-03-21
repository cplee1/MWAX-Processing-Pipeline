process MOVE_DATA {
    tag "${obsid_to_move}"

    input:
    val(ready)
    val(jobid)
    val(vcs_dir)
    val(asvo_dir)
    val(vcs_obsid)
    val(obsid_to_move)
    val(mode)

    output:
    val(true), emit: ready

    script:
    if ( mode == 'vcs' ) {
    """
        if [[ ! -d ${vcs_dir} ]]; then
            echo "Error: VCS directory does not exist."
            exit 1
        fi

        # Create a directory to move files into
        mkdir -p -m 771 ${vcs_dir}/${obsid_to_move}/combined

        # Move data

        if [ -e ${asvo_dir}/${jobid}/*.sub ]; then
            mv ${asvo_dir}/${jobid}/*.sub ${vcs_dir}/${obsid_to_move}/combined
        fi

        if [ -e ${asvo_dir}/${jobid}/*.dat ]; then
            mv ${asvo_dir}/${jobid}/*.dat ${vcs_dir}/${obsid_to_move}/combined
        fi

        if [ -e ${asvo_dir}/${jobid}/*.metafits ]; then
            mv ${asvo_dir}/${jobid}/*.metafits ${vcs_dir}/${obsid_to_move}
        fi
        
        # Delete the job directory
        if [[ -d ${asvo_dir}/${jobid} ]]; then
            if [[ -r ${asvo_dir}/${jobid}/MWA_ASVO_README.md ]]; then
                rm ${asvo_dir}/${jobid}/MWA_ASVO_README.md
            fi
            if [[ -z "\$(ls -A ${asvo_dir}/${jobid})" ]]; then
                rmdir ${asvo_dir}/${jobid}
            else
                echo "Job directory not empty: ${asvo_dir}/${jobid}."
            fi
        fi
        """
    } else if ( mode == 'vis' ) {
        """
        if [[ ! -d ${vcs_dir} ]]; then
            echo "Error: VCS directory does not exist."
            exit 1
        fi

        # Create a directory to move files into
        mkdir -p -m 771 ${asvo_dir}/${jobid}/${vcs_obsid}/cal/${obsid_to_move}

        # Move data
        mv ${asvo_dir}/${jobid}/* ${asvo_dir}/${jobid}/${vcs_obsid}/cal/${obsid_to_move}

        # Delete the job directory
        if [[ -d ${asvo_dir}/${jobid} ]]; then
            if [[ -r ${asvo_dir}/${jobid}/MWA_ASVO_README.md ]]; then
                rm ${asvo_dir}/${jobid}/MWA_ASVO_README.md
            fi
            if [[ -z "\$(ls -A ${asvo_dir}/${jobid})" ]]; then
                rmdir ${asvo_dir}/${jobid}
            else
                echo "Job directory not empty: ${asvo_dir}/${jobid}."
            fi
        fi
        """
    } else {
        System.err.println("ERROR: Invalid file transfer mode encountered: ${mode}")
    }
}
