process MOVE_DATA {
    tag "${obsid_to_move}"

    input:
    val(jobid)
    val(vcs_dir)
    val(asvo_dir)
    val(vcs_obsid)
    val(mode)
    tuple val(obsid_to_move), val(format)

    output:
    val(true), emit: ready

    script:
    if ( mode == 'vcs' ) {
    """
        if [[ ! -d ${vcs_dir} ]]; then
            echo "Error: VCS directory does not exist."
            exit 1
        fi

        # Move the data
        if grep -q "combined" "${format}"; then
            mkdir -p -m 771 ${vcs_dir}/${obsid_to_move}/combined
            if [ -e \$(find ${asvo_dir}/${jobid} -type f -name *.sub | head -n1) ]; then
                mv ${asvo_dir}/${jobid}/*.sub ${vcs_dir}/${obsid_to_move}/combined
            else
                echo "Error: No data files found."
                exit 1
            fi
        elif grep -q "raw" "${format}"; then
            mkdir -p -m 771 ${vcs_dir}/${obsid_to_move}/raw
            if [ -e \$(find ${asvo_dir}/${jobid} -type f -name *.dat | head -n1) ]; then
                mv ${asvo_dir}/${jobid}/*.dat ${vcs_dir}/${obsid_to_move}/combined
            else
                echo "Error: No data files found."
                exit 1
            fi
        else
            echo "Error: Invalid data format."
            exit 1
        fi

        # Move the metafits
        if [ -e ${asvo_dir}/${jobid}/${obsid_to_move}.metafits ]; then
            mv ${asvo_dir}/${jobid}/${obsid_to_move}.metafits ${vcs_dir}/${obsid_to_move}
        fi
        if [ -e ${asvo_dir}/${jobid}/${obsid_to_move}_metafits_ppds.fits ]; then
            mv ${asvo_dir}/${jobid}/${obsid_to_move}_metafits_ppds.fits ${vcs_dir}/${obsid_to_move}
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
