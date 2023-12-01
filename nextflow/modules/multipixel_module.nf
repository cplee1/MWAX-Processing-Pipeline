#!/usr/bin/env nextflow

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
    shell '/bin/bash', '-veuo', 'pipefail'

    input:
    tuple val(RAJ), val(DECJ)

    output:
    path("pointings_${task.index}.txt")

    script:
    """
    # Determine a unique glob
    IFS=':' read -r raj_hours raj_minutes raj_seconds <<< "${RAJ}"
    IFS=':' read -r decj_degrees decj_minutes decj_seconds <<< "${DECJ}"
    pointing_glob="*\$raj_hours:\$raj_minutes:*\$decj_degrees:\$decj_minutes:*"

    # Write label and individual coordinates to file
    echo "${RAJ} ${DECJ} ${RAJ}_${DECJ} \$pointing_glob" | tee pointings_${task.index}.txt

    # Ensure that there is a directory for the beamformed data
    psr_dir="${params.vcs_dir}/${params.obsid}/pointings/${RAJ}_${DECJ}/psrfits_${params.duration}s"
    if [[ ! -d \$psr_dir ]]; then
        mkdir -p -m 771 \$psr_dir
    fi

    # Move any existing beamformed data into a subdirectory
    old_files=\$(find \$psr_dir -type f)
    if [[ -n \$old_files ]]; then
        archive="\${psr_dir}/beamformed_data_archived_\$(date +%s)"
        mkdir -p -m 771 \$archive
        echo \$old_files | xargs -n1 mv -t \$archive
    fi
    """
}

process combine_pointings {
    shell '/bin/bash', '-veuo', 'pipefail'

    input:
    path(pointings_files)

    output:
    tuple path('labels.txt'), path('pointings.txt'), path('pointing_pairs.txt'), path('flagged_tiles.txt')

    script:
    if ( params.convert_rts_flags ) {
        """
        if [[ -z ${params.obsid} || -z ${params.calid} ]]; then
            echo "Error: Please provide ObsID and CalID."
            exit 1
        fi

        if [[ ! -d ${params.vcs_dir}/${params.obsid} ]]; then
            echo "Error: Cannot find observation directory."
            exit 1
        fi

        # Combine pointings into appropriate file structures
        files=\$(find pointings*.txt)
        echo \$files | xargs -n1 cat | tee -a pointings_labels.txt

        # Make text files to give to VCSBeam
        cat pointings_labels.txt | awk '{print \$1" "\$2}' > pointings.txt
        cat pointings_labels.txt | awk '{print \$3" "\$4}' > pointing_pairs.txt
        cat pointings_labels.txt | awk '{print \$3}' > labels.txt

        # Write the tile flags to file
        echo "${params.flagged_tiles}" | tee flagged_tiles_rts.txt
        ${params.convert_flags_script} \
            -m ${params.vcs_dir}/${params.obsid}/cal/${params.calid}/${params.calid}.metafits \
            -i flagged_tiles_rts.txt \
            -o flagged_tiles.txt
        """
    } else {
        """
        if [[ -z ${params.obsid} || -z ${params.calid} ]]; then
            echo "Error: Please provide ObsID and CalID."
            exit 1
        fi

        if [[ ! -d ${params.vcs_dir}/${params.obsid} ]]; then
            echo "Error: Cannot find observation directory."
            exit 1
        fi

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

process get_pointings {
    label 'psranalysis'

    shell '/bin/bash', '-veuo', 'pipefail'

    input:
    val(psrs)

    output:
    tuple val(psrs), path('pointings.txt'), path('pointing_pairs.txt'), path('flagged_tiles.txt')

    script:
    """
    if [[ -z ${params.obsid} || -z ${params.calid} ]]; then
        echo "Error: Please provide ObsID and CalID."
        exit 1
    fi

    if [[ ! -d ${params.vcs_dir}/${params.obsid} ]]; then
        echo "Error: Cannot find observation directory."
        exit 1
    fi

    # Turn the Nextflow list into a Bash array
    eval "pulsars=(\$(echo ${psrs} | sed 's/\\[//;s/\\]//;s/,/ /g'))"

    for (( i=0; i<\${#pulsars[@]}; i++ )); do
        # Get equatorial coordinates from the catalogue
        RAJ=\$(psrcat -e2 "\${pulsars[i]}" | grep "RAJ " | awk '{print \$2}')
        DECJ=\$(psrcat -e2 "\${pulsars[i]}" | grep "DECJ " | awk '{print \$2}')
        if [[ -z \$RAJ || -z \$DECJ ]]; then
            echo "Error: Could not retrieve pointing from psrcat."
            exit 1
        fi
        # Write equatorial coordinates to file
        echo "\${RAJ} \${DECJ}" | tee -a pointings.txt

        # Determine a unique glob for each pointing
        IFS=':' read -r raj_hours raj_minutes raj_seconds <<< "\$RAJ"
        IFS=':' read -r decj_degrees decj_minutes decj_seconds <<< "\$DECJ"
        pointing_glob="*\$raj_hours:\$raj_minutes:*\$decj_degrees:\$decj_minutes:*"
        # Write globs to file
        echo "\${pulsars[i]} \${pointing_glob}" | tee -a pointing_pairs.txt

        # Ensure that there is a directory for the beamformed data
        psr_dir="${params.vcs_dir}/${params.obsid}/pointings/\${pulsars[i]}/psrfits_${params.duration}s"
        if [[ ! -d \$psr_dir ]]; then
            mkdir -p -m 771 \$psr_dir
        fi

        # Move any existing beamformed data into a subdirectory
        old_files=\$(find \$psr_dir -type f)
        if [[ -n \$old_files ]]; then
            archive="\${psr_dir}/beamformed_data_archived_\$(date +%s)"
            mkdir -p -m 771 \$archive
            echo \$old_files | xargs -n1 mv -t \$archive
        fi
    done

    # Write the tile flags to file
    echo "${params.flagged_tiles}" | tee flagged_tiles.txt
    """
}

process vcsbeam {
    label 'gpu'
    label 'vcsbeam'

    shell '/bin/bash', '-veuo', 'pipefail'

    maxForks 1

    time { 4.hour * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'finish' }
    maxRetries 1

    input:
    tuple val(psrs), val(pointings), val(pairs), val(flagged_tiles)

    output:
    val(psrs)

    script:
    """
    if [[ -z \$(cat ${pairs}) ]]; then
        echo "Error: Pointing globs file is empty."
        exit 1
    fi

    if [[ ! -r ${params.vcs_dir}/${params.obsid}/${params.obsid}.metafits || \
        ! -r ${params.vcs_dir}/${params.obsid}/cal/${params.calid}/${params.calid}.metafits || \
        ! -r ${params.vcs_dir}/${params.obsid}/cal/${params.calid}/hyperdrive/hyperdrive_solutions.bin ]]; then
        echo "Error: Cannot locate input files for VCSBeam."
        exit 1
    fi

    make_mwa_tied_array_beam -V
    echo "\$(date): Executing make_mwa_tied_array_beam."
    srun -N ${params.num_chan} -n ${params.num_chan} make_mwa_tied_array_beam \
        -n 10 \
        -m ${params.vcs_dir}/${params.obsid}/${params.obsid}.metafits \
        -b ${params.begin} \
        -T ${params.duration} \
        -f ${params.low_chan} \
        -d ${params.vcs_dir}/${params.obsid}/combined \
        -P ${pointings} \
        -F ${flagged_tiles} \
        -c ${params.vcs_dir}/${params.obsid}/cal/${params.calid}/${params.calid}.metafits \
        -C ${params.vcs_dir}/${params.obsid}/cal/${params.calid}/hyperdrive/hyperdrive_solutions.bin \
        -R NONE -U 0,0 -O -X --smart -p
    echo "\$(date): Finished executing make_mwa_tied_array_beam."

    # Turn the Nextflow list into a Bash array
    eval "pulsars=(\$(echo ${psrs} | sed 's/\\[//;s/\\]//;s/,/ /g'))"

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

include { locate_fits_files; get_ephemeris; prepfold } from './singlepixel_module'

// Beamform and fold on catalogued pulsar in PSRFITS/multipixel mode
workflow mpsr {
    take:
        // Channel where each item is a list of pulsar J names
        psrs
    main:
        // Beamform once
        get_pointings(psrs)
            | vcsbeam
            | flatten
            | locate_fits_files
            | get_ephemeris
            | prepfold
}

// Beamform on pointing in PSRFITS/multipixel mode
workflow mpt {
    take:
        // Channel where each item is a list of pointings
        pointings
    main:
        // Beamform on each pointing
        parse_pointings(pointings)
            | collect
            | combine_pointings
            | map { files -> [files[0].splitCsv().flatten().collect(), files[1], files[2], files[3]] }
            | vcsbeam
}
