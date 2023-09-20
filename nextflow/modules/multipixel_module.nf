#!/usr/bin/env nextflow

/*
    Multipixel Module
    ~~~~~~~~~~~~~~~~~~
    This module contains processes and workflows for beamforming on multiple
    pulsars using the multipixel beamformer (one big job). The beamformed data
    are then separated and the folding is done independently for each pulsar.
    
    Since the multipixel beamformer is only compatible with detected output,
    this workflow assumes PSRFITS output.
*/

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
        file_format='psrfits'
        psr_dir="${params.vcs_dir}/${params.obsid}/pointings/\${pulsars[i]}/\${file_format}_${params.duration}s"
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

    time { 4.hour * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    maxRetries 1

    input:
    tuple val(psrs), val(pointings), val(pairs), val(flagged_tiles)

    output:
    val psrs, emit: psrs
    val pairs, emit: pairs
    path '*.fits', emit: paths

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
        file_format='psrfits'
        find . -type f -name "\${pointing_glob}" -exec cp {} "${params.vcs_dir}/${params.obsid}/pointings/\${pulsars[i]}/\${file_format}_${params.duration}s" \\;
    done
    """
}

process get_ephemeris {
    label 'psranalysis'

    shell '/bin/bash', '-veuo', 'pipefail'

    input:
    val(psr)
    val(pairs)
    val(vcsbeam_files)

    output:
    tuple val(psr), val(pairs), val(vcsbeam_files), path("${psr}.par")

    script:
    """
    if [[ -z ${psr} ]]; then
        echo "Error: Pulsar name string is blank."
        exit 1
    fi

    par_file=${psr}.par

    if [[ -r ${params.ephemeris_dir}/\$par_file ]]; then
        # Preference is to use MeerTime ephemeris
        cp ${params.ephemeris_dir}/\$par_file \$par_file
    else
        # Otherwise, use ATNF catalogue
        echo "MeerKAT ephemeris not found. Using PSRCAT."
        psrcat -v
        psrcat -e ${psr} > \$par_file
        if [[ ! -z \$(grep WARNING \$par_file) ]]; then
            echo "Error: Pulsar not in catalogue."
            exit 1
        fi
    fi

    # TCB time standard causes problems in tempo/prepfold
    time_standard=\$(cat \$par_file | grep UNITS | awk '{print \$2}')
    if [[ \$time_standard == 'TCB' ]]; then
        par_file_tcb=${psr}_TCB.par
        mv \$par_file \$par_file_tcb
        tempo2 -gr transform \$par_file_tcb \$par_file back
    fi
    
    # Replace TAI with BIPM
    sed -i "s/TT(TAI)/TT(BIPM)/" \$par_file
    
    # Replace BIPMyyyy with BIPM
    sed -i 's/TT(BIPM[0-9]\\{4\\})/TT(BIPM)/g' \$par_file
    """
}

process prepfold {
    label 'cpu'
    label 'psrsearch'
    label 'prepfold'

    shell '/bin/bash', '-veuo', 'pipefail'

    time 1.hour

    errorStrategy { task.attempt == 1 ? 'retry' : 'ignore' }
    maxRetries 1

    input:
    tuple val(psr), val(pairs), path(vcsbeam_files), path(par_file)

    script:
    """
    # Locate fits files
    find -L . -type f -name "\$(grep ${psr} ${pairs} | awk '{print \$2}').fits" | xargs -n1 basename | sort > fitsfiles.txt

    if [[ -z \$(cat fitsfiles.txt) ]]; then
        echo "Error: No fits files found."
        exit 1
    fi

    bin_flag=""
    if [[ ! -z \$(grep BINARY ${par_file}) ]]; then
        bin_flag="-bin"
    fi

    par_input=""
    if [[ ${task.attempt} == 1 ]]; then
        # On first attempt, try the par file
        if [[ \$(cat ${par_file} | grep BINARY | awk '{print \$2}') == 'T2' ]]; then
            echo "Binary model T2 not accepted by TEMPO."
            # Default to PRESTO ephemeris
            par_input="-psr ${psr}"
        else
            par_input="-par ${par_file}"
        fi
    else
        # Otherwise, try the inbuilt ephermeris in PRESTO
        par_input="-psr ${psr}"
    fi

    spin_freq=\$(grep F0 ${par_file} | awk '{print \$2}')
    spin_period_ms=\$(echo "scale=5; 1000 / \$spin_freq" | bc)
    if [[ -z \$spin_period_ms ]]; then
        echo "Error: Cannot locate spin period."
        exit 1
    elif (( \$(echo "\$spin_period_ms < ${params.nbin}/10" | bc -l) )); then
        # Set nbins to 10x the period in ms, and always round down
        nbin=\$(printf "%.0f" \$(echo "scale=0; 10 * \$spin_period_ms - 0.5" | bc))
    else
        nbin=${params.nbin}
    fi

    prepfold \
        -ncpus ${task.cpus} \
        \$par_input \
        -noxwin \
        -noclip \
        -n \$nbin \
        -nsub ${params.nsub} \
        -npart ${params.npart} \
        \$bin_flag \
        \$(cat fitsfiles.txt)

    dataproduct_dir=${params.vcs_dir}/${params.obsid}/pointings/${psr}/psrfits_${params.duration}s
    if [[ ! -d \${dataproduct_dir}/prepfold ]]; then
        mkdir -p -m 771 \${dataproduct_dir}/prepfold
    fi

    # Move files to publish directory
    mv *pfd* \${dataproduct_dir}/prepfold
    cat fitsfiles.txt | xargs -n1 cp -L -t \$dataproduct_dir
    cat fitsfiles.txt | xargs -n1 readlink -f | xargs -n1 rm
    """
}

// User the multipixel beamformer (assumes PSRFITS format)
workflow beamform_mp {
    take:
        // Channel where each item is a list of pulsar Jnames
        psrs
    main:
        // Beamform once
        get_pointings(psrs) | vcsbeam | set { vcsbeam_out }

        vcsbeam_out.psrs
            .flatten()
            .set { psrs_flat }
        
        // Fold each pulsar
        get_ephemeris(psrs_flat, vcsbeam_out.pairs, vcsbeam_out.paths) | prepfold
}