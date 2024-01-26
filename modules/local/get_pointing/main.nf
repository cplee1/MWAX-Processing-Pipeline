process GET_POINTING {
    label 'psranalysis'

    tag "${psr}"

    input:
    val(psr)
    val(calmeta)
    val(flagged_tiles)

    output:
    path('pointings.txt'), emit: pointings
    path('flagged_tiles.txt'), emit: flagged_tiles

    script:
    if ( params.convert_rts_flags ) {
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

        # Write the tile flags to file
        echo "${flagged_tiles}" | tee flagged_tiles.txt

        # Write the tile flags to file
        echo "${flagged_tiles}" | tee flagged_tiles_rts.txt
        ${params.convert_flags_script} \\
            -m ${calmeta} \\
            -i flagged_tiles_rts.txt \\
            -o flagged_tiles.txt

        """
    } else {
        """
        RAJ=\$(psrcat -e2 ${psr} | grep "RAJ " | awk '{print \$2}')
        DECJ=\$(psrcat -e2 ${psr} | grep "DECJ " | awk '{print \$2}')
        if [[ -z \$RAJ || -z \$DECJ ]]; then
            echo "Error: Could not retrieve pointing from psrcat."
            exit 1
        fi
        # Write equatorial coordinates to file
        echo "\${RAJ} \${DECJ}" | tee pointings.txt

        # Write the tile flags to file
        echo "${flagged_tiles}" | tee flagged_tiles.txt
        """

    }
}
