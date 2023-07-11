#!/usr/bin/env nextflow

bf_out = ' -v '
if ( params.fits ) {
    bf_out = ' -p '
}

process get_pointings {
    label 'psranalysis'

    input:
    val(psrs)

    output:
    tuple val(psrs), path('pointings.txt'), path('pointing_pairs.txt'), path('flagged_tiles.txt')

    script:
    """
    set -eux

    if [[ -z ${params.obsid} || -z ${params.calid} ]]; then
        echo "Error: Please provide obsid and calid."
        exit 1
    fi

    if [[ ! -d ${params.vcs_dir}/${params.obsid} ]]; then
        echo "Error: Cannot find observation directory."
        exit 1
    fi

    # Turn the Nextflow list into a Bash array
    PSRS="${psrs}"
    PSRS="\${PSRS:1:-1}"
    PSRS="\${PSRS//,/ }"
    eval "PSRS=(\$PSRS)"

    for (( i=0; i<\${#PSRS[@]}; i++ )); do
        RAJ=\$(psrcat -e2 "\${PSRS[i]}" | grep "RAJ " | awk '{print \$2}')
        DECJ=\$(psrcat -e2 "\${PSRS[i]}" | grep "DECJ " | awk '{print \$2}')
        
        if [[ -z \$RAJ || -z \$DECJ ]]; then
            echo "Error: Could not retrieve pointing from psrcat."
            exit 1
        fi

        echo "\${RAJ} \${DECJ}" | tee -a pointings.txt

        IFS=':' read -r raj_hours raj_minutes raj_seconds <<< "\$RAJ"
        IFS=':' read -r decj_degrees decj_minutes decj_seconds <<< "\$DECJ"
        raj_seconds_rounded=\$(printf "%05.2f" "\$raj_seconds")
        decj_seconds_rounded=\$(printf "%05.2f" "\$decj_seconds")
        POINTING="\$raj_hours:\$raj_minutes:\$raj_seconds_rounded"_"\$decj_degrees:\$decj_minutes:\$decj_seconds_rounded"

        echo "\${PSRS[i]} \${POINTING}" | tee -a pointing_pairs.txt

        PSR_DIR="${params.vcs_dir}/${params.obsid}/pointings/\${PSRS[i]}"
        if [[ ! -d \$PSR_DIR ]]; then
            mkdir -p -m 771 \$PSR_DIR
        fi

        OLD_FILES=\$(find \$PSR_DIR -type f -name "*.{fits,vdif,hdr}")
        if [[ -n \$OLD_FILES ]]; then
            ARCHIVE="\${PSR_DIR}/archived_\$(date +%s)"
            mkdir -p -m 771 \$ARCHIVE
            find \$PSR_DIR -type f -name "*.{fits,vdif,hdr}" -exec mv {} \$ARCHIVE \\;
        fi
    done

    echo "${params.flagged_tiles.split(',').join(' ')}" | tee flagged_tiles.txt
    """
}

process vcsbeam {
    label 'gpu'
    label 'vcsbeam'

    time { 2.hour * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    maxRetries 2

    input:
    tuple val(psrs), val(pointings), val(pairs), val(flagged_tiles)

    output:
    val psrs, emit: pulsars
    val pairs, emit: pairs
    path '*.{fits,vdif,hdr}', emit: paths

    script:
    """
    set -eux
    which make_mwa_tied_array_beam

    if [[ -z \$(cat ${pairs}) ]]; then
        echo "Error: Pointings file empty."
        exit 1
    fi

    if [[ ! -r ${params.vcs_dir}/${params.obsid}/${params.obsid}.metafits || \
          ! -r ${params.vcs_dir}/${params.obsid}/cal/${params.calid}/${params.calid}.metafits || \
          ! -r ${params.vcs_dir}/${params.obsid}/cal/${params.calid}/hyperdrive/hyperdrive_solutions.bin ]]; then
        echo "Error: Cannot find files for VCSBeam."
        exit 1
    fi

    echo "\$(date): Executing make_mwa_tied_array_beam."
    srun -N 24 -n 24 make_mwa_tied_array_beam \
        -n 10 \
        -m ${params.vcs_dir}/${params.obsid}/${params.obsid}.metafits \
        -b ${params.startgps} \
        -T ${params.duration} \
        -f ${params.low_chan} \
        -d ${params.vcs_dir}/${params.obsid}/combined \
        -P ${pointings} \
        -F ${flagged_tiles} \
        -c ${params.vcs_dir}/${params.obsid}/cal/${params.calid}/${params.calid}.metafits \
        -C ${params.vcs_dir}/${params.obsid}/cal/${params.calid}/hyperdrive/hyperdrive_solutions.bin \
        -R NONE -U 0,0 -O -X --smart ${bf_out}

    echo "\$(date): Finished executing make_mwa_tied_array_beam."

    # Turn the Nextflow list into a Bash array
    PSRS="${psrs}"
    PSRS="\${PSRS:1:-1}"
    PSRS="\${PSRS//,/ }"
    eval "PSRS=(\$PSRS)"

    for (( i=0; i<\${#PSRS[@]}; i++ )); do
        POINTING=\$(grep "\${PSRS[i]}" ${pairs})
        if [[ -z \$POINTING ]]; then
            echo "Error: Cannot find pointing for pulsar \${PSRS[i]}."
            exit 1
        fi
    
        find . -type f -name "*\${POINTING}*" -exec cp {} "${params.vcs_dir}/${params.obsid}/pointings/\${PSRS[i]}" \\;
    done
    """
}

process get_ephemeris {
    label 'psranalysis'

    input:
    val psr
    val pairs
    val vcsbeam_files

    output:
    val psr
    val pairs
    val vcsbeam_files
    path "${psr}.par"

    script:
    """
    if [[ -z ${psr} ]]; then
        echo "Error: Pulsar name string is blank."
        exit 1
    fi

    par_file=${psr}.par

    psrcat -e ${psr} > \$par_file

    if [[ ! -z \$(grep WARNING \$par_file) ]]; then
        echo "Error: Pulsar not in catalogue."
        psrcat -v
        exit 1
    fi  

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

    time { 1.hour * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    maxRetries 2

    input:
    val psr
    val pairs
    path vcsbeam_files
    path par_file

    when:
    params.fits == false

    script:
    """
    set -eux
    
    # Locate header and voltage files
    find -L . -type f -name "*\$(grep ${psr} ${pairs} | awk '{print \$2}')*.hdr" | xargs -n1 basename | sort > headers.txt
    find -L . -type f -name "*\$(grep ${psr} ${pairs} | awk '{print \$2}')*.vdif" | xargs -n1 basename | sort > vdiffiles.txt

    if [[ -z \$(cat headers.txt) ]]; then
        echo "Error: No header files found."
        exit 1
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
                    -b ${params.nbin} \
                    -U \$size_mb \
                    -F ${params.nchan}:D \
                    -L ${params.tint} -A \
                    -O \$outfile \
                    \$datafile_hdr
            fi
        fi
    done

    base_name=${psr}_bins${params.nbin}_fchans${params.nchan}_tint${params.tint}

    # Stitch together channels
    psradd -R -o \${base_name}.ar *.ar

    # Flag first time integration
    paz -s 0 -m \${base_name}.ar

    # Plotting
    pav -FTpC -D -g \${base_name}_pulse_profile.png/png \${base_name}.ar
    pav -TpC -G -g \${base_name}_waterfall.png/png \${base_name}.ar
    pav -FpC -Y -g \${base_name}_waterfall.png/png \${base_name}.ar

    dataproduct_dir=${params.vcs_dir}/${params.obsid}/pointings/${psr}/vdif_${params.duration}s
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

process prepfold {
    label 'cpu'
    label 'psrsearch'
    label 'prepfold'

    time { 1.hour * task.attempt }

    errorStrategy {
        if (task.exitStatus == 2) {
            'ignore'
        } else if (task.exitStatus in 137..140) {
            'retry'
        } else {
            'terminate'
        }
    }
    maxRetries 2

    input:
    val psr
    val pairs
    path vcsbeam_files
    path par_file

    when:
    params.fits == true

    script:
    """
    set -eux

    if [[ \$(cat ${par_file} | grep BINARY | awk '{print \$2}') == 'T2' ]]; then
        echo "Error: Binary model T2 not accepted by TEMPO."
        exit 2
    fi

    # Locate fits files
    find -L . -type f -name "*\$(grep ${psr} ${pairs} | awk '{print \$2}')*.fits" | xargs -n1 basename | sort > fitsfiles.txt

    if [[ -z \$(cat fitsfiles.txt) ]]; then
        echo "Error: No fits files found."
        exit 1
    fi

    bin_flag=""
    if [[ ! -z \$(grep BINARY ${par_file}) ]]; then
        bin_flag="-bin"
    fi
    
    prepfold \
        -ncpus ${task.cpus} \
        -par ${par_file} \
        -noxwin \
        -noclip \
        -n ${params.nbin} \
        -nsub 256 \
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

// PIPELINE OPTIONS
// ---------------------------------------------------------------------
// Observation options:
// --obsid OBSID                          (REQUIRED!)
// --calid CALID                          (REQUIRED!)
// --startgps STARTGPS                    (DEFAULT: OBSID)
// --duration DURATION                    (DEFAULT: 592)
// --low_chan LOW_CHAN                    (DEFAULT: 109)
// --flagged_tiles TILE1,[TILE2,TILE3]    (DEFAULT: None)
//
// Beamforming options:
// --psrs PSR1,[PSR2,PSR3,...,PSRN]       (REQUIRED!)
// --fits                                 (DEFAULT: false)
//
// Post-processing options:
// --nbin NBIN                            (DEFAULT: 128)
// --nchan NCHAN                          (DEFAULT: 128)
// --tint TINT                            (DEFAULT: 8 seconds)
// ---------------------------------------------------------------------

workflow {
    Channel
        .from( params.psrs.split(',') )
        .collect()
        .set { psrs }

    get_pointings(psrs) | vcsbeam | set { vcsbeam_out }

    vcsbeam_out.pulsars
        .flatten()
        .set { pulsars }

    if ( params.fits ) {
        get_ephemeris(pulsars, vcsbeam_out.pairs, vcsbeam_out.paths) | prepfold
    }
    else {
        get_ephemeris(pulsars, vcsbeam_out.pairs, vcsbeam_out.paths) | dspsr
    }
}