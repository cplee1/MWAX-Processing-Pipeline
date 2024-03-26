process PARSE_TILE_FLAGS {
    input:
    val(calmeta)
    val(flagged_tiles)

    output:
    path('flagged_tiles.txt')

    script:
    if ( params.convert_rts_flags ) {
        """
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
        # Write the tile flags to file
        echo "${flagged_tiles}" | tee flagged_tiles.txt
        """

    }
}
