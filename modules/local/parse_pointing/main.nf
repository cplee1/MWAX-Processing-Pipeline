process PARSE_POINTING {
    input:
    tuple val(RAJ), val(DECJ)

    output:
    tuple val("${RAJ}_${DECJ}"), path('pointings.txt')

    script:
    """
    # Write equatorial coordinates to file
    echo "${RAJ} ${DECJ}" | tee pointings.txt
    """
}
