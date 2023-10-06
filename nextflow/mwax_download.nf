#!/usr/bin/env nextflow

def help_message() {
    log.info """
        |mwax_download.nf: Download VCS observations and calibration observations
        |and move the data into the standard directory structure.
        |
        |USAGE:
        |   mwax_download.nf [OPTIONS]
        |
        |   Note: Space separated lists must be enclosed in quotes.
        |
        |VOLTAGE DOWNLOAD OPTIONS:
        |   --obsid <OBSID>
        |       ObsID of the VCS observation. [no default]
        |   --duration <DURATION>
        |       Length of time to download in seconds. [no default]
        |   --offset <OFFSET>
        |       Offset from the start of the observation in seconds. [no default]
        |   --asvo_api_key <ASVO_API_KEY>
        |       API key corresponding to the user's ASVO account. [no default]
        |
        |CALIBRATOR DOWNLOAD OPTIONS:
        |   --calids <CALIDS>...
        |       Space separated list of ObsIDs of calibrator observations. [no default]
        |
        |FILE MOVING OPTIONS:
        |   --skip_download
        |       Skip download. Instead, move files which are already downloaded.
        |   --asvo_id_obs <ASVO_ID_OBS>
        |       ASVO job ID of the downloaded VCS observation. [no default]
        |   --asvo_id_cals <ASVO_ID_CALS>...
        |       Space separated list of ASVO job IDs of the downloaded calibrator
        |       observations. [no default]
        |
        |PIPELINE OPTIONS:
        |   --help
        |       Print this help information.
        |   --asvo_dir <ASVO_DIR>
        |       Path to where ASVO downloads are stored.
        |       [default: ${params.asvo_dir}]
        |   --vcs_dir <VCS_DIR>
        |       Path to where VCS data files will be stored.
        |       [default: ${params.vcs_dir}]
        |   -w <WORK_DIR>
        |       The Nextflow work directory. Delete the directory once the
        |       process is finished. [default: ${workDir}]
        |
        |EXAMPLES:
        |1. Typical usage
        |   mwax_download.nf --obsid 1372184672 --calids "1372184552 1372189472"
        |   --duration 600 --offset 0 --asvo_api_key <API_KEY>
        |2. Move files which are already downloaded
        |   mwax_download.nf --skip_download --asvo_id_obs 661635 --asvo_id_cals "661634 661636"
        """.stripMargin()
}

if ( params.help ) {
    help_message()
    exit(0)
}

process asvo_vcs_download {
    label 'giant_squid'
    
    tag "${obsid}"

    shell '/bin/bash', '-veu'

    time 2.day
    maxRetries 5

    errorStrategy {
        if ( task.exitStatus == 75 ) {
            wait_minutes = 30 * Math.pow(2, task.attempt)
            log.info("Sleeping for ${wait_hours} minutes and retrying task ${task.hash}")
            sleep(wait_minutes * 60 * 1000 as long)
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

    time 12.hour
    maxRetries 5

    errorStrategy {
        if ( task.exitStatus == 75 ) {
            wait_hours = Math.pow(2, task.attempt)
            log.info("Sleeping for ${wait_hours} hours and retrying task ${task.hash}")
            sleep(wait_hours * 60 * 60 * 1000 as long)
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
    if [[ \$(find ${fpath} "*.metafits" | wc -l) != 1 ]]; then
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
        if [[ -d ${fpath} && -z "\$(ls -A ${fpath})" ]]; then
            rm -r ${fpath}
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
        if [[ -d ${fpath} && -z "\$(ls -A ${fpath})" ]]; then
            rm -r ${fpath}
        fi
        """
    } else {
        println "Invalid file transfer mode encountered: ${mode}."
    }
}

workflow mv {
    main:
        if ( params.obsid ) {
            Channel
                .from( params.asvo_id_obs )
                .map { jobid -> [ jobid, "${params.asvo_dir}/${jobid}", 'vcs' ] }
                .set { vcs_job }

            check_asvo_job_files(vcs_job) | check_obsid | move_data | set { obsid }
        }

        if ( params.calids ) {
            Channel
                .from( params.asvo_id_cals.split(' ') )
                .map { jobid -> [ jobid, "${params.asvo_dir}/${jobid}", 'vis' ] }
                .set { cal_jobs }

            check_asvo_job_files(cal_jobs) | check_obsid | move_data | set { obsid }
        }
    emit:
        obsid = obsid
}

workflow dl {
    main:
        if ( params.obsid ) {
            asvo_vcs_download(params.obsid) | check_asvo_job_files | check_obsid | move_data | set { obsid }
        }

        if ( params.calids ) {
            Channel
                .from( params.calids.split(' ') )
                .set { calids_in }

            asvo_vis_download(calids_in) | check_asvo_job_files | check_obsid | move_data | set { obsid }
        }
    emit:
        obsid = obsid
}

workflow {
    if ( ! params.obsid && ! params.calids ) {
        println "Please specify obs IDs with --obsid or --calids."
    } else {
        if ( params.skip_download ) {
            if ( ! params.asvo_id_obs && ! params.asvo_id_cals ) {
                println "Please specify ASVO job IDs with --asvo_id_obs and --asvo_id_cals."
            } else {
                mv()  // Move data
            }
        } else {
            if ( ! params.asvo_api_key ) {
                println "Please specify ASVO API key with --asvo_api_key."
            } else {
                if ( ! params.duration && ! params.offset ) {
                    println "Please specify the duration and offset with --duration and --offset."
                } else {
                    dl()  // Download and move data
                }
            }
        }
    }
}