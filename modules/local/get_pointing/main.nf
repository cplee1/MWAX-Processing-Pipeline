process GET_POINTING {
    label 'psranalysis'

    tag "${psr}"

    input:
    val(psr)

    output:
    tuple val(psr), path('pointings.txt'), emit: pointings

    script:
    """
    # Get equatorial coordinates
    RAJ=\$(psrcat -e2 ${psr} | grep "RAJ " | awk '{print \$2}')
    DECJ=\$(psrcat -e2 ${psr} | grep "DECJ " | awk '{print \$2}')
    if [[ -z \$RAJ || -z \$DECJ ]]; then
        echo "Error: Could not retrieve pointing from psrcat."
        exit 1
    fi
    # Write equatorial coordinates to file
    echo "\${RAJ} \${DECJ}" | tee pointings.txt
    """
}
