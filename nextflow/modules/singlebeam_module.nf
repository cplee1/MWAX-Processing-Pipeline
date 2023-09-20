#!/usr/bin/env nextflow

process get_pointings {
    label 'psranalysis'

    shell '/bin/bash', '-veuo', 'pipefail'

    input:
    val psr

    output:
    tuple val(psr), path('pointings.txt'), path('flagged_tiles.txt')

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

    RAJ=\$(psrcat -e2 ${psr} | grep "RAJ " | awk '{print \$2}')
    DECJ=\$(psrcat -e2 ${psr} | grep "DECJ " | awk '{print \$2}')
    if [[ -z \$RAJ || -z \$DECJ ]]; then
        echo "Error: Could not retrieve pointing from psrcat."
        exit 1
    fi
    # Write equatorial coordinates to file
    echo "\${RAJ} \${DECJ}" | tee pointings.txt

    # Ensure that there is a directory for the beamformed data
    file_format='vdif'
    psr_dir="${params.vcs_dir}/${params.obsid}/pointings/${psr}/\${file_format}_${params.duration}s"
    if [[ ! -d \$psr_dir ]]; then
        mkdir -p -m 771 \$psr_dir
    fi

    # Move any existing beamformed data into a subdirectory
    old_files=\$(find \$psr_dir -type f -name "*.{vdif,hdr}")
    if [[ -n \$old_files ]]; then
        archive="\${psr_dir}/archived_\$(date +%s)"
        mkdir -p -m 771 \$archive
        find \$psr_dir -type f -name "*.{vdif,hdr}" -exec mv {} \$archive \\;
    fi

    # Write the tile flags to file
    echo "${params.flagged_tiles}" | tee flagged_tiles.txt
    """
}

process vcsbeam {
    label 'gpu'
    label 'vcsbeam'

    shell '/bin/bash', '-veuo', 'pipefail'

    time { 1.hour * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    maxRetries 1

    input:
    tuple val(psr), val(pointings), val(flagged_tiles)

    output:
    tuple val(psr), path('*.{vdif,hdr}')

    script:
    """
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
        -R NONE -U 0,0 -O -X --smart -v
    echo "\$(date): Finished executing make_mwa_tied_array_beam."
    """
}

process get_ephemeris {
    label 'psranalysis'

    shell '/bin/bash', '-veuo', 'pipefail'

    input:
    tuple val(psr), val(vcsbeam_files)

    output:
    tuple val(psr), val(vcsbeam_files), path("${psr}.par")

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

process dspsr {
    label 'cpu'
    label 'psranalysis'
    label 'dspsr'
    
    shell '/bin/bash', '-veuo', 'pipefail'

    time { 1.hour * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'ignore' }
    maxRetries 2

    input:
    tuple val(psr), path(vcsbeam_files), path(par_file)

    script:
    """
    find *.hdr | xargs -n1 basename > headers.txt
    find *.vdif | xargs -n1 basename > vdiffiles.txt

    if [[ -z \$(cat headers.txt) ]]; then
        echo "Error: No header files found."
        exit 1
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

    for datafile_hdr in `awk '{ print \$1 }' headers.txt | paste -s -d ' '`; do
        if [ ! -s \$datafile_hdr ]; then
            echo "Error: Invalid hdr file \'\${datafile_hdr}\'. Skipping file."
        else
            datafile_vdif=\${datafile_hdr%.hdr}.vdif
            if [ ! -s \$datafile_vdif ]; then
                echo "Error: Invalid vdif file \'\${datafile_vdif}\'. Skipping file."
            else
                size_mb=4096
                outfile=\${datafile_hdr%.hdr}
                dspsr \
                    -E ${par_file} \
                    -b \$nbin \
                    -U \$size_mb \
                    -F ${params.fine_chan}:D \
                    -L ${params.tint} -A \
                    -O \$outfile \
                    \$datafile_hdr
            fi
        fi
    done

    # Make a list of channel archives to delete
    find *.ar | xargs -n1 basename > channel_archives.txt

    # The name of the combined archive
    base_name=${psr}_bins\${nbin}_fchans${params.fine_chan}_tint${params.tint}

    # Stitch together channels and delete individual channel archives
    psradd -R -o \${base_name}.ar *.ar
    cat channel_archives.txt | xargs rm
    rm channel_archives.txt

    # Flag first time integration
    paz -s 0 -m \${base_name}.ar

    # Plotting
    pav -FTpC -D -g \${base_name}_pulse_profile.png/png \${base_name}.ar
    pav -TpC -G -g \${base_name}_frequency_phase.png/png \${base_name}.ar
    pav -FpC -Y -g \${base_name}_time_phase.png/png \${base_name}.ar

    file_format='vdif'
    dataproduct_dir=${params.vcs_dir}/${params.obsid}/pointings/${psr}/\${file_format}_${params.duration}s
    if [[ ! -d \${dataproduct_dir}/dspsr ]]; then
        mkdir -p -m 771 \${dataproduct_dir}/dspsr
    fi

    # Move files to publish directory
    mv *.ar *.png \${dataproduct_dir}/dspsr
    cat vdiffiles.txt | xargs -n1 cp -L -t \$dataproduct_dir
    cat vdiffiles.txt | xargs -n1 readlink -f | xargs -n1 rm
    cat headers.txt | xargs -n1 cp -L -t \$dataproduct_dir
    cat headers.txt | xargs -n1 readlink -f | xargs -n1 rm
    """
}

// Use the standard beamformer (assumes VDIF format)
workflow beamform {
    take:
        // Channel of individual pulsar Jnames
        psrs
    main:
        // Beamform and fold each pulsar
        get_pointings(psrs) | vcsbeam | get_ephemeris | dspsr
}