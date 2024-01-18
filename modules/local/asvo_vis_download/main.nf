process ASVO_VIS_DOWNLOAD {
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
    tuple env(jobid), env(fpath), val('vis')

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
