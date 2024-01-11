process asvo_vcs_download {
    label 'giant_squid'
    
    tag "${obsid}"

    shell '/bin/bash', '-veu'

    time 2.day
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
    tuple env(jobid), env(fpath), env(mode)

    script:
    """
    export MWA_ASVO_API_KEY="${params.asvo_api_key}"
    mode=vcs

    # Submit job and supress failure if job already exists
    ${params.giant_squid} submit-volt -v -w \
        --delivery astro \
        --offset ${params.offset} \
        --duration ${params.duration} \
        ${obsid} \
        || true

    ${params.giant_squid} list -j \
        --types DownloadVoltage \
        --states Ready \
        -- ${obsid} \
        | tee /dev/stderr \
        | ${params.jq} -r '.[]|[.jobId,.files[0].filePath//"",.files[0].fileSize//""]|@tsv' \
        | sort -r \
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

    shell '/bin/bash', '-veu'

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
    tuple env(jobid), env(fpath), env(mode)

    script:
    """
    export MWA_ASVO_API_KEY="${params.asvo_api_key}"
    mode=vis

    # Submit job and supress failure if job already exists
    ${params.giant_squid} submit-vis -v -w \
        --delivery astro \
        ${obsid} \
        || true

    ${params.giant_squid} list -j \
        --types DownloadVisibilities \
        --states Ready \
        -- ${obsid} \
        | tee /dev/stderr \
        | ${params.jq} -r '.[]|[.jobId,.files[0].filePath//"",.files[0].fileSize//""]|@tsv' \
        | sort -r \
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
    shell '/bin/bash', '-veuo', 'pipefail'

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
    tuple val(jobid), val(fpath), val(mode)

    output:
    tuple val(jobid), val(fpath), env(obsid), val(mode)

    script:
    """
    # Does the directory exist?
    if [[ ! -d "${fpath}" ]]; then
        echo "Error: ASVO job directory does not exist."
        exit 2
    fi

    # Is there data files in the directory?
    if [[ \$(find ${fpath} -name "*.sub"  | wc -l) -lt 1 && \
          \$(find ${fpath} -name "*.dat"  | wc -l) -lt 1 && \
          \$(find ${fpath} -name "*.fits" | wc -l) -lt 1 ]]; then
        echo "Error: Cannot locate data files."
        exit 3
    fi

    # Is there a metafits file in the directory?
    if [[ \$(find ${fpath} -name "*.metafits" | wc -l) != 1 ]]; then
        echo "Error: Cannot locate metafits file."
        exit 4
    fi

    # Get the obsid
    if [[ -z "${params.obsid}" ]]; then
        obsid=\$(find ${fpath} -name "*.metafits" | xargs -n1 basename -s ".metafits")
    else
        obsid="${params.obsid}"
        if [[ \${#obsid} != 10 ]]; then
            echo "Error: The provided obs ID is not valid."
            exit 5
        fi
    fi
    """
}

process check_obsid {
    tag "${obsid}"

    input:
    tuple val(jobid), val(fpath), val(obsid), val(mode)

    output:
    tuple val(jobid), val(fpath), val(obsid), val(mode)

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
    shell '/bin/bash', '-veuo', 'pipefail'

    tag "${obsid}"

    input:
    tuple val(jobid), val(fpath), val(obsid), val(mode)

    output:
    val(params.obsid)

    script:
    if ( mode == 'vcs' ) {
    """
        if [[ ! -d ${params.vcs_dir} ]]; then
            echo "Error: VCS directory does not exist."
            exit 1
        fi

        # Create a directory to move files into
        mkdir -p -m 771 ${params.vcs_dir}/${obsid}/combined

        # Move data
        mv ${fpath}/*.sub ${params.vcs_dir}/${obsid}/combined
        mv ${fpath}/*.metafits ${params.vcs_dir}/${obsid}
        
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
        if [[ ! -d ${params.vcs_dir} ]]; then
            echo "Error: VCS directory does not exist."
            exit 1
        fi

        # Create a directory to move files into
        mkdir -p -m 771 ${params.vcs_dir}/${params.obsid}/cal/${obsid}

        # Move data
        mv ${fpath}/* ${params.vcs_dir}/${params.obsid}/cal/${obsid}

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
        println "Invalid file transfer mode encountered: ${mode}."
    }
}
