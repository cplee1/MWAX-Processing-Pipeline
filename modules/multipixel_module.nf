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
    tag 'multi-psr'
    
    input:
    tuple val(RAJ), val(DECJ), val(obsmeta), val(calmeta), val(calsol)

    output:
    path("pointings_${task.index}.txt"), emit: pointing_info
    tuple val(obsmeta), val(calmeta), val(calsol), emit: job_info

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

    tag 'multi-psr'

    input:
    tuple val(psr), val(obsmeta), val(calmeta), val(calsol)

    output:
    path("pointings_${task.index}.txt"), emit: pointing_info
    tuple val(obsmeta), val(calmeta), val(calsol), emit: job_info

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
    tag 'multi-psr'

    input:
    path(pointing_info_files)
    tuple val(obsmeta), val(calmeta), val(calsol)

    output:
    tuple val(obsmeta), val(calmeta), val(calsol), path('labels.txt'), path('pointings.txt'), path('pointing_pairs.txt'), path('flagged_tiles.txt')

    script:
    if ( params.convert_rts_flags ) {
        """
        # Combine pointings into appropriate file structures
        files=\$(find pointings*.txt)
        echo \$files | xargs -n1 cat | tee -a pointings_labels.txt

        # Make text files to give to VCSBeam
        cat pointings_labels.txt | awk '{print \$1" "\$2}' > pointings.txt
        cat pointings_labels.txt | awk '{print \$3" "\$4}' > pointing_pairs.txt
        cat pointings_labels.txt | awk '{print \$3}' > labels.txt

        # Write the tile flags to file
        echo "${params.flagged_tiles}" | tee flagged_tiles_rts.txt
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
        cat pointings_labels.txt | awk '{print \$3}' > labels.txt

        # Write the tile flags to file
        echo "${params.flagged_tiles}" | tee flagged_tiles.txt
        """
    }
}

process vcsbeam {
    label 'gpu'
    label 'vcsbeam'

    tag 'multi-psr'

    maxForks 1

    time { 1.hour * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'finish' }
    maxRetries 1

    input:
    tuple val(sources), val(obsmeta), val(calmeta), val(calsol), val(pointings), val(pairs), val(flagged_tiles)

    output:
    val(sources)

    script:
    """
    if [[ -z \$(cat ${pairs}) ]]; then
        echo "Error: Pointing globs file is empty."
        exit 1
    fi

    make_mwa_tied_array_beam -V
    echo "\$(date): Executing make_mwa_tied_array_beam."
    srun make_mwa_tied_array_beam \\
        -m ${obsmeta} \\
        -b ${params.begin} \\
        -T ${params.duration} \\
        -f ${params.low_chan} \\
        -d ${params.vcs_dir}/${params.obsid}/combined \\
        -P ${pointings} \\
        -F ${flagged_tiles} \\
        -c ${calmeta} \\
        -C ${calsol} \\
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
        find . -type f -name "\$pointing_glob" -exec mv -t "${params.vcs_dir}/${params.obsid}/pointings/\${pulsars[i]}/psrfits_${params.duration}s" '{}' \\;
    done
    """
}

include { check_directories          } from './singlepixel_module'
include { get_calibration_solution   } from './singlepixel_module'
include { locate_fits_files          } from './singlepixel_module'
include { get_ephemeris              } from './singlepixel_module'
include { prepfold                   } from './singlepixel_module'

// Beamform and fold on catalogued pulsar in PSRFITS/multipixel mode
workflow mpsr {
    take:
        // Channel of [source, obsmeta, calmeta, calsol]
        job_info
    main:
        // Beamform once
        get_pointings(job_info)

        combine_pointings(get_pointings.out.pointing_info.collect(), get_pointings.out.job_info.first())
            | map { [ it[3].splitCsv().flatten().collect(), it[0], it[1], it[2], it[4], it[5], it[6] ] }
            | vcsbeam
            | flatten
            | locate_fits_files
            | get_ephemeris
            | prepfold
}

// Beamform on pointing in PSRFITS/multipixel mode
workflow mpt {
    take:
        // Channel of [source, obsmeta, calmeta, calsol]
        job_info
    main:
        // Beamform on each pointing
        parse_pointings(job_info)

        combine_pointings(parse_pointings.out.pointing_info.collect(), parse_pointings.out.job_info.first())
            | map { [ it[3].splitCsv().flatten().collect(), it[0], it[1], it[2], it[4], it[5], it[6] ] }
            | vcsbeam
}
