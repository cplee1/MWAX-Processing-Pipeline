process GET_POINTINGS {
    label 'psranalysis'

    input:
    val(psr)

    output:
    path("pointings_${task.index}.txt")

    script:
    """
    # Get equatorial coordinates
    RAJ=\$(psrcat -e2 ${psr} | grep "RAJ " | awk '{print \$2}')
    DECJ=\$(psrcat -e2 ${psr} | grep "DECJ " | awk '{print \$2}')
    if [[ -z \$RAJ || -z \$DECJ ]]; then
        echo "Error: Could not retrieve pointing from psrcat."
        exit 1
    fi

    # Determine a unique glob for each pointing
    IFS=':' read -r raj_hours raj_minutes raj_seconds <<< "\$RAJ"
    IFS=':' read -r decj_degrees decj_minutes decj_seconds <<< "\$DECJ"
    pointing_glob="*\$raj_hours:\$raj_minutes:*\${decj_degrees#-}:\$decj_minutes*"
    
    # Write Jname and individual coordinates to file
    echo "\$RAJ \$DECJ ${psr} \$pointing_glob" | tee pointings_${task.index}.txt
    """
}
