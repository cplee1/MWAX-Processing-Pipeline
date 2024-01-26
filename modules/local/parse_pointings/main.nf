process PARSE_POINTINGS {
    input:
    tuple val(RAJ), val(DECJ)

    output:
    path("pointings_${task.index}.txt")

    script:
    """
    # Label for naming files and directories
    pointing_label="${RAJ}_${DECJ}"

    # Determine a unique glob
    IFS=':' read -r raj_hours raj_minutes raj_seconds <<< "${RAJ}"
    IFS=':' read -r decj_degrees decj_minutes decj_seconds <<< "${DECJ}"
    pointing_glob="*\$raj_hours:\$raj_minutes:*\$decj_degrees:\$decj_minutes:*"

    # Write label and individual coordinates to file
    echo "${RAJ} ${DECJ} ${RAJ}_${DECJ} \$pointing_glob" | tee pointings_${task.index}.txt
    """
}
