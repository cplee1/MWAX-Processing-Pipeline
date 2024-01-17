process COMBINE_POINTINGS {
    input:
    path(pointing_info_files)
    val(calmeta)
    val(flagged_tiles)

    output:
    path('pointings.txt'), emit: pointings
    path('pointing_pairs.txt'), emit: pairs
    path('flagged_tiles.txt'), emit: flagged_tiles
    

    script:
    if ( params.convert_rts_flags ) {
        """
        # Combine pointings into appropriate file structures
        files=\$(find pointings*.txt)
        echo \$files | xargs -n1 cat | tee -a pointings_labels.txt

        # Make text files to give to VCSBeam
        cat pointings_labels.txt | awk '{print \$1" "\$2}' > pointings.txt
        cat pointings_labels.txt | awk '{print \$3" "\$4}' > pointing_pairs.txt

        # Write the tile flags to file
        echo "${flagged_tiles}" | tee flagged_tiles_rts.txt
        ${params.convert_flags_script} \\
            -m ${calmeta} \\
            -i flagged_tiles_rts.txt \
            -o flagged_tiles.txt
        """
    } else {
        """
        # Combine pointings into appropriate file structures
        files=\$(find pointings*.txt)
        echo \$files | xargs -n1 cat | tee -a pointings_labels.txt

        # Make text files to give to VCSBeam
        cat pointings_labels.txt | awk '{print \$1" "\$2}' > pointings.txt
        cat pointings_labels.txt | awk '{print \$3" "\$4}' > pointing_pairs.txt

        # Write the tile flags to file
        echo "${flagged_tiles}" | tee flagged_tiles.txt
        """
    }
}
