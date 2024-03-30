process CHECK_ASVO_JOB_FILES {
    errorStrategy {
        failure_reason = [
            2: "ASVO job directory does not exist",
            3: "cannot locate data files",
            4: "cannot locate metafits file",
        ][task.exitStatus] ?: "unknown"
        println "Task ${task.hash} failed with code ${task.exitStatus}: ${failure_reason}."
        return 'terminate'
    }

    input:
    tuple val(job_id), val(dl_path), val(job_size)
    
    output:
    val(true)

    script:
    """
    # Does the directory exist?
    if [[ ! -d ${dl_path} ]]; then
        echo "Error: ASVO job directory does not exist."
        exit 2
    fi

    # Is there data files in the directory?
    if [[ \$(find ${dl_path} -name "*.sub"  | wc -l) -lt 1 && \\
        \$(find ${dl_path} -name "*.dat"  | wc -l) -lt 1 && \\
        \$(find ${dl_path} -name "*.fits" | wc -l) -lt 1 ]]; then
        echo "Error: Cannot locate data files."
        exit 3
    fi

    # Is there a metafits file in the directory?
    if [[ \$(find ${dl_path} -name "*.metafits" | wc -l) != 1 ]]; then
        echo "Error: Cannot locate metafits file."
        exit 4
    fi
    """
}
