process ASVO_VCS_DOWNLOAD {
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