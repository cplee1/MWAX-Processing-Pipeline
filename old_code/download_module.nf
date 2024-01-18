/*
Download Module
~~~~~~~~~~~~~~~~~~
This module contains processes for downloading and moving data.
*/

process asvo_vcs_download {
    label 'giant_squid'
    
    tag "${obsid}"

    time 2.day
    maxRetries 5

    errorStrategy {
        if ( task.exitStatus == 75 ) {
            wait_hours = Math.pow(2, task.attempt - 1 as int)
            log.info("Sleeping for ${wait_hours} hours and retrying task ${task.hash}")
            sleep(wait_hours * 60 * 60 * 1000 as long)
            return 'retry'
        }
        log.info("task ${task.hash} failed with code ${task.exitStatus}")
        return 'ignore'
    }

    input:
    val(obsid)
    val(offset)
    val(duration)

    output:
    env(jobid), emit: jobid
    env(fpath), emit: asvo_path
    val('vcs'), emit: mode

    script:
    """
    export MWA_ASVO_API_KEY="${params.asvo_api_key}"

    # Submit job and supress failure if job already exists
    ${params.giant_squid} submit-volt -v -w \\
        --delivery astro \\
        --offset ${offset} \\
        --duration ${duration} \\
        ${obsid} \\
        || true

    ${params.giant_squid} list -j \\
        --types DownloadVoltage \\
        --states Ready \\
        -- ${obsid} \\
        | tee /dev/stderr \\
        | ${params.jq} -r '.[]|[.jobId,.files[0].filePath//"",.files[0].fileSize//""]|@tsv' \\
        | sort -r \\
        | tee ready.tsv

    if read -r jobid fpath fsize < ready.tsv; then
        echo "Job \${jobid} is ready in directory \${fpath}."
    else
        echo "No jobs are ready."
        exit 75
    fi
    """
}

process asvo_vis_download {
    label 'giant_squid'
    
    tag "${obsid}"

    time 1.day
    maxRetries 5

    errorStrategy {
        if ( task.exitStatus == 75 ) {
            wait_hours = Math.pow(2, task.attempt - 1 as int)
            log.info("Sleeping for ${wait_hours} minutes and retrying task ${task.hash}")
            sleep(wait_hours * 60 * 1000 as long)
            return 'retry'
        }
        log.info("task ${task.hash} failed with code ${task.exitStatus}")
        return 'ignore'
    }

    input:
    val(obsid)

    output:
    env(jobid), emit: jobid
    env(fpath), emit: asvo_path
    val('vis'), emit: mode

    script:
    """
    export MWA_ASVO_API_KEY="${params.asvo_api_key}"

    # Submit job and supress failure if job already exists
    ${params.giant_squid} submit-vis -v -w \\
        --delivery astro \\
        ${obsid} \\
        || true

    ${params.giant_squid} list -j \\
        --types DownloadVisibilities \\
        --states Ready \\
        -- ${obsid} \\
        | tee /dev/stderr \\
        | ${params.jq} -r '.[]|[.jobId,.files[0].filePath//"",.files[0].fileSize//""]|@tsv' \\
        | sort -r \\
        | tee ready.tsv

    if read -r jobid fpath fsize < ready.tsv; then
        echo "Job \${jobid} is ready in directory \${fpath}."
    else
        echo "No jobs are ready."
        exit 75
    fi
    """
}

process check_asvo_job_files {
    tag "${jobid}"

    errorStrategy {
        failure_reason = [
            2: "ASVO job directory does not exist",
            3: "cannot locate data files",
            4: "cannot locate metafits file",
            5: "the specified obs ID is not valid",
        ][task.exitStatus] ?: "unknown"
        println "Task ${task.hash} failed with code ${task.exitStatus}: ${failure_reason}."
        return 'terminate'
    }

    input:
    val(jobid)
    val(asvo_path)

    output:
    env(obsid)

    script:
    """
    # Does the directory exist?
    if [[ ! -d "${asvo_path}" ]]; then
        echo "Error: ASVO job directory does not exist."
        exit 2
    fi

    # Is there data files in the directory?
    if [[ \$(find ${asvo_path} -name "*.sub"  | wc -l) -lt 1 && \\
          \$(find ${asvo_path} -name "*.dat"  | wc -l) -lt 1 && \\
          \$(find ${asvo_path} -name "*.fits" | wc -l) -lt 1 ]]; then
        echo "Error: Cannot locate data files."
        exit 3
    fi

    # Is there a metafits file in the directory?
    if [[ \$(find ${asvo_path} -name "*.metafits" | wc -l) != 1 ]]; then
        echo "Error: Cannot locate metafits file."
        exit 4
    fi

    # Get the obsid
    obsid=\$(find ${asvo_path} -name "*.metafits" | xargs -n1 basename -s ".metafits")

    if [[ \${#obsid} != 10 ]]; then
        echo "Error: The provided obs ID is not valid."
        exit 5
    fi
    """
}

process check_obsid {
    tag "${obsid}"

    input:
    val(obsid)

    output:
    val(true)

    script:
    """
    #!/usr/bin/env python
    import sys

    def check_obsid(string):
        if string.isdigit() and len(string) == 10:
            return True
        else:
            return False

    if not (check_obsid('${obsid}')):
        sys.exit(1)
    """
}

process move_data {
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
