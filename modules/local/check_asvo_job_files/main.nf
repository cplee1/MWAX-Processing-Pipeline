process CHECK_ASVO_JOB_FILES {
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
