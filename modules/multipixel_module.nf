/*
Multipixel Module
~~~~~~~~~~~~~~~~~~
This module contains processes and workflows for beamforming on multiple
pulsars using the multipixel beamformer (one big job). The beamformed data
are then separated and the folding is done independently for each pulsar.

Since the multipixel beamformer is only compatible with PSRFITS output,
this workflow assumes PSRFITS.
*/

process parse_pointings {
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

process get_pointings {
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
    pointing_glob="*\$raj_hours:\$raj_minutes:*\$decj_degrees:\$decj_minutes:*"
    
    # Write Jname and individual coordinates to file
    echo "\$RAJ \$DECJ ${psr} \$pointing_glob" | tee pointings_${task.index}.txt
    """
}

process combine_pointings {
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

process vcsbeam {
    label 'gpu'
    label 'vcsbeam'

    maxForks 1

    time { 1.hour * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'finish' }
    maxRetries 1

    input:
    val(sources)
    val(pointings_dir)
    val(data_dir)
    val(duration)
    val(begin)
    val(low_chan)
    val(obs_metafits)
    val(cal_metafits)
    val(cal_solution)
    val(flagged_tiles)
    val(pointings)
    val(pairs)

    output:
    val(true)

    script:
    """
    make_mwa_tied_array_beam -V
    echo "\$(date): Executing make_mwa_tied_array_beam."
    srun make_mwa_tied_array_beam \\
        -m ${obs_metafits} \\
        -b ${begin} \\
        -T ${duration} \\
        -f ${low_chan} \\
        -d ${data_dir} \\
        -P ${pointings} \\
        -F ${flagged_tiles} \\
        -c ${cal_metafits} \\
        -C ${cal_solution} \\
        -R NONE -U 0,0 -O -X --smart -p
    echo "\$(date): Finished executing make_mwa_tied_array_beam."

    # Turn the Nextflow list into a Bash array
    eval "pulsars=(\$(echo ${sources} | sed 's/\\[//;s/\\]//;s/,/ /g'))"

    # Organise files by pointing using the specified globs
    for (( i=0; i<\${#pulsars[@]}; i++ )); do
        pointing_glob=\$(grep "\${pulsars[i]}" ${pairs} | awk '{print \$2}')
        if [[ -z \$pointing_glob ]]; then
            echo "Error: Cannot find pointing for pulsar \${pulsars[i]}."
            exit 1
        fi
        find . -type f -name "\$pointing_glob" -exec mv -t "${pointings_dir}/\${pulsars[i]}/psrfits_${duration}s" '{}' \\;
    done
    """
}
