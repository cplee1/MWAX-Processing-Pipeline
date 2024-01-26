process PARSE_POINTING {
    input:
    tuple val(RAJ), val(DECJ)
    val(calmeta)
    val(flagged_tiles)

    output:
    path('pointings.txt'), emit: pointings
    path('flagged_tiles.txt'), emit: flagged_tiles

    script:
    if ( params.convert_rts_flags ) {
        """
        # Label for naming files and directories
        pointing_label="${RAJ}_${DECJ}"

        # Write equatorial coordinates to file
        echo "${RAJ} ${DECJ}" | tee pointings.txt

        # Write the tile flags to file
        echo "${flagged_tiles}" | tee flagged_tiles_rts.txt
        ${params.convert_flags_script} \\
            -m ${calmeta} \\
            -i flagged_tiles_rts.txt \\
            -o flagged_tiles.txt
        """
    } else {
        """
        # Label for naming files and directories
        pointing_label="${RAJ}_${DECJ}"

        # Write equatorial coordinates to file
        echo "${RAJ} ${DECJ}" | tee pointings.txt

        # Write the tile flags to file
        echo "${flagged_tiles}" | tee flagged_tiles.txt
        """
    }
}
