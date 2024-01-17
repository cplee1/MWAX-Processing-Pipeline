/*
Singlepixel Module
~~~~~~~~~~~~~~~~~~
This module contains processes for beamforming and folding on 
multiple pulsars independently of one another. Each pulsar is assigned its
own beamforming job which is then fed into a folding job.

Since we use the multipixel beamformer for PSRFITS output, the singlepixel
beamforming workflow assumes VDIF output. The locate_fits_files and prepfold
processes are included here for if the use wants to re-fold their PSRFITS
data, which does not involve the multipixel beamformer.
*/

def publish_vdif = params.psrs != null || params.acacia_prefix_base != null ? false : true

process locate_vdif_files {
    tag "${source}"

    input:
    val(source)
    val(source_dir)
    val(duration)

    output:
    path('*.{vdif,hdr}')

    script:
    """
    vdif_dir="${source_dir}/vdif_${duration}s"
    if [[ ! -d \$psr_dir ]]; then
        echo "ERROR :: Cannot locate data directory: \${vdif_dir}"
        exit 1
    fi
    find \$vdif_dir -type f -name "*.vdif" -exec ln -s '{}' \\;
    find \$vdif_dir -type f -name "*.hdr" -exec ln -s '{}' \\;

    if [[ -d \${vdif_dir}/dspsr ]]; then
        old_dspsr_files=\$(find \${vdif_dir}/dspsr -type f)
        if [[ -n \$old_dspsr_files ]]; then
            archive="\${vdif_dir}/dspsr_archived_\$(date +%s)"
            mkdir -p -m 771 \$archive
            echo \$old_dspsr_files | xargs -n1 mv -t \$archive
        fi
    fi
    """
}

process locate_psrfits_files {
    tag "${source}"

    input:
    val(ready)
    val(source)
    val(source_dir)
    val(duration)

    output:
    path('*.fits')

    script:
    """
    psrfits_dir="${source_dir}/psrfits_${duration}s"
    if [[ ! -d \$psrfits_dir ]]; then
        echo "Error: Cannot locate data directory."
        exit 1
    fi
    find \$psrfits_dir -type f -name "*.fits" -exec ln -s '{}' \\;

    if [[ -d \${psrfits_dir}/prepfold ]]; then
        old_prepfold_files=\$(find \${psrfits_dir}/prepfold -type f)
        if [[ -n \$old_prepfold_files ]]; then
            archive="\${psrfits_dir}/prepfold_archived_\$(date +%s)"
            mkdir -p -m 771 \$archive
            echo \$old_prepfold_files | xargs -n1 mv -t \$archive
        fi
    fi
    """
}

process check_directories {
    tag "${source}"

    input:
    val(ready)
    val(do_fits)
    val(do_vdif)
    val(base_dir)
    val(obsid)
    val(source)
    val(duration)

    output:
    env(data_dir), emit: data_dir
    env(pointings_dir), emit: pointings_dir
    env(source_dir), emit: source_dir

    script:
    if ( do_fits && do_vdif ) {
        """
        if [[ ! -d ${base_dir} ]]; then
            echo "ERROR :: Base directory does not exist: ${base_dir}"
            exit 1
        fi

        if [[ ! -d ${base_dir}/${obsid} ]]; then
            echo "ERROR :: Observation directory does not exist: ${base_dir}/${obsid}"
            exit 1
        fi

        if [[ ! -d ${base_dir}/${obsid}/combined ]]; then
            echo "ERROR :: Data directory does not exist: ${base_dir}/${obsid}/combined"
            exit 1
        fi

        data_dir="${base_dir}/${obsid}/combined"
        pointings_dir="${base_dir}/${obsid}/pointings"
        source_dir="\${pointings_dir}/${source}"

        psrfits_dir="\${source_dir}/psrfits_${duration}s"
        if [[ ! -d \$psrfits_dir ]]; then
            mkdir -p -m 771 \$psrfits_dir
        fi
        old_psrfits_files=\$(find \$psrfits_dir -type f)
        if [[ -n \$old_psrfits_files ]]; then
            psrfits_archive="\${psrfits_dir}/beamformed_data_archived_\$(date +%s)"
            mkdir -p -m 771 \$psrfits_archive
            echo \$old_psrfits_files | xargs -n1 mv -t \$psrfits_archive
        fi

        vdif_dir="\${source_dir}/vdif_${duration}s"
        if [[ ! -d \$vdif_dir ]]; then
            mkdir -p -m 771 \$vdif_dir
        fi
        old_vdif_files=\$(find \$vdif_dir -type f)
        if [[ -n \$old_vdif_files ]]; then
            vdif_archive="\${vdif_dir}/beamformed_data_archived_\$(date +%s)"
            mkdir -p -m 771 \$vdif_archive
            echo \$old_vdif_files | xargs -n1 mv -t \$vdif_archive
        fi
        """
    } else if ( do_fits ) {
        """
        if [[ ! -d ${base_dir} ]]; then
            echo "ERROR :: Base directory does not exist: ${base_dir}"
            exit 1
        fi

        if [[ ! -d ${base_dir}/${obsid} ]]; then
            echo "ERROR :: Observation directory does not exist: ${base_dir}/${obsid}"
            exit 1
        fi

        if [[ ! -d ${base_dir}/${obsid}/combined ]]; then
            echo "ERROR :: Data directory does not exist: ${base_dir}/${obsid}/combined"
            exit 1
        fi

        data_dir="${base_dir}/${obsid}/combined"
        pointings_dir="${base_dir}/${obsid}/pointings"
        source_dir="\${pointings_dir}/${source}"

        psrfits_dir="\${source_dir}/psrfits_${duration}s"
        if [[ ! -d \$psrfits_dir ]]; then
            mkdir -p -m 771 \$psrfits_dir
        fi
        old_psrfits_files=\$(find \$psrfits_dir -type f)
        if [[ -n \$old_psrfits_files ]]; then
            psrfits_archive="\${psrfits_dir}/beamformed_data_archived_\$(date +%s)"
            mkdir -p -m 771 \$psrfits_archive
            echo \$old_psrfits_files | xargs -n1 mv -t \$psrfits_archive
        fi
        """
    } else if ( do_vdif ) {
        """
        if [[ ! -d ${base_dir} ]]; then
            echo "ERROR :: Base directory does not exist: ${base_dir}"
            exit 1
        fi

        if [[ ! -d ${base_dir}/${obsid} ]]; then
            echo "ERROR :: Observation directory does not exist: ${base_dir}/${obsid}"
            exit 1
        fi

        if [[ ! -d ${base_dir}/${obsid}/combined ]]; then
            echo "ERROR :: Data directory does not exist: ${base_dir}/${obsid}/combined"
            exit 1
        fi

        data_dir="${base_dir}/${obsid}/combined"
        pointings_dir="${base_dir}/${obsid}/pointings"
        source_dir="\${pointings_dir}/${source}"

        vdif_dir="\${source_dir}/vdif_${duration}s"
        if [[ ! -d \$vdif_dir ]]; then
            mkdir -p -m 771 \$vdif_dir
        fi
        old_vdif_files=\$(find \$vdif_dir -type f)
        if [[ -n \$old_vdif_files ]]; then
            vdif_archive="\${vdif_dir}/beamformed_data_archived_\$(date +%s)"
            mkdir -p -m 771 \$vdif_archive
            echo \$old_vdif_files | xargs -n1 mv -t \$vdif_archive
        fi
        """
    }
}

process get_calibration_solution {
    input:
    val(obsid)
    val(calid)

    output:
    env(obsmeta), emit: obsmeta
    env(calmeta), emit: calmeta
    env(calsol), emit: calsol

    script:
    if ( params.use_default_sol ) {
        """
        if [[ ! -r ${params.vcs_dir}/${obsid}/${obsid}.metafits ]]; then
            echo "VCS observation metafits not found. Exiting."
            exit 1
        fi

        if [[ ! -r ${params.calsol_dir}/${obsid}/${calid}/${calid}.metafits ]]; then
            echo "Calibration observation metafits not found. Exiting."
            exit 1
        fi

        if [[ ! -e ${params.calsol_dir}/${obsid}/${calid}/hyperdrive/hyperdrive_solutions.bin ]]; then
            echo "Default calibration solution not found. Exiting."
            exit 1
        fi

        obsmeta="${params.vcs_dir}/${obsid}/${obsid}.metafits"
        calmeta="${params.calsol_dir}/${obsid}/${calid}/${calid}.metafits"
        calsol="${params.calsol_dir}/${obsid}/${calid}/hyperdrive/hyperdrive_solutions.bin"
        """
    } else {
        """
        if [[ ! -r ${params.vcs_dir}/${obsid}/${obsid}.metafits ]]; then
            echo "VCS observation metafits not found. Exiting."
            exit 1
        fi

        if [[ ! -r ${params.vcs_dir}/${obsid}/cal/${calid}/${calid}.metafits ]]; then
            echo "Calibration observation metafits not found. Exiting."
            exit 1
        fi

        if [[ ! -r ${params.vcs_dir}/${obsid}/cal/${calid}/hyperdrive/hyperdrive_solutions.bin ]]; then
            echo "Calibration solution not found. Exiting."
            exit 1
        fi

        obsmeta="${params.vcs_dir}/${obsid}/${obsid}.metafits"
        calmeta="${params.vcs_dir}/${obsid}/cal/${calid}/${calid}.metafits"
        calsol="${params.vcs_dir}/${obsid}/cal/${calid}/hyperdrive/hyperdrive_solutions.bin"
        """
    }
}

process parse_pointings {
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

process get_pointings {
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

process vcsbeam {
    label 'gpu'
    label 'vcsbeam'

    tag "${psr}"

    maxForks 3

    time { 1.hour * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'finish' }
    maxRetries 1

    publishDir "${source_dir}/vdif_${duration}s", mode: 'move', enabled: publish_vdif

    input:
    val(psr)
    val(source_dir)
    val(data_dir)
    val(duration)
    val(begin)
    val(low_chan)
    val(obs_metafits)
    val(cal_metafits)
    val(cal_solution)
    val(flagged_tiles)
    val(pointings)

    output:
    path('*.{vdif,hdr}')

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
        -R NONE -U 0,0 -O -X --smart -v
    echo "\$(date): Finished executing make_mwa_tied_array_beam."
    """
}

process get_ephemeris {
    label 'psranalysis'

    tag "${psr}"

    errorStrategy {
        log.info("task ${task.hash} failed with code ${task.exitStatus}")
        if ( task.exitStatus == 2 ) {
            log.info('Pulsar name string is blank.')
        } else if ( task.exitStatus == 3 ) {
            log.info("Pulsar ${psr} not found in catalogue.")
        }
        return 'ignore'
    }

    input:
    val(psr)
    val(vcsbeam_files)
    val(ephemeris_dir)
    val(force_psrcat)

    output:
    path("${psr}.par")

    script:
    if ( force_psrcat ) {
        """
        if [[ -z ${psr} ]]; then
            echo "Error: Pulsar name string is blank."
            exit 2
        fi

        par_file=${psr}.par
        psrcat -v || true
        psrcat -e ${psr} > \$par_file
        if [[ ! -z \$(grep WARNING \$par_file) ]]; then
            echo "Error: Pulsar not in catalogue."
            exit 3
        fi

        if [[ "${vcsbeam_files[0]}" == *.fits ]]; then
            # TEMPO1 compatibility modifications
            # ----------------------------------
            # Convert TCB to TDB
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
        fi
        """
    } else {
        """
        if [[ -z ${psr} ]]; then
            echo "Error: Pulsar name string is blank."
            exit 2
        fi

        par_file=${psr}.par

        if [[ -r ${ephemeris_dir}/\$par_file ]]; then
            # Preference is to use MeerTime ephemeris
            cp ${ephemeris_dir}/\$par_file \$par_file
        else
            # Otherwise, use ATNF catalogue
            echo "MeerKAT ephemeris not found. Using PSRCAT."
            psrcat -v || true
            psrcat -e ${psr} > \$par_file
            if [[ ! -z \$(grep WARNING \$par_file) ]]; then
                echo "Error: Pulsar not in catalogue."
                exit 3
            fi
        fi

        if [[ "${vcsbeam_files[0]}" == *.fits ]]; then
            # TEMPO1 compatibility modifications
            # ----------------------------------
            # Convert TCB to TDB
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
        fi
        """
    }
}


process dspsr {
    label 'cpu'
    label 'psranalysis'
    label 'dspsr'

    tag "${psr}"

    time { 2.hour * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'ignore' }
    maxRetries 2

    input:
    val(psr)
    val(source_dir)
    val(duration)
    val(nbin)
    val(fine_chan)
    val(tint)
    path(vcsbeam_files)
    path(par_file)

    output:
    val(true)

    script:
    """
    find *.hdr | sort > headers.txt
    find *.vdif | sort > vdiffiles.txt

    if [[ -z \$(cat headers.txt) ]]; then
        echo "Error: No header files found."
        exit 1
    fi

    spin_freq=\$(grep F0 ${par_file} | awk '{print \$2}')
    spin_period_ms=\$(echo "scale=5; 1000 / \$spin_freq" | bc)
    if [[ -z \$spin_period_ms ]]; then
        echo "Error: Cannot locate spin period."
        exit 1
    elif (( \$(echo "\$spin_period_ms < ${nbin}/20" | bc -l) )); then
        # Set nbins to 20x the period in ms, and always round down
        nbin=\$(printf "%.0f" \$(echo "scale=0; 20 * \$spin_period_ms - 0.5" | bc))
    else
        nbin=${nbin}
    fi

    for datafile_hdr in \$(cat headers.txt); do
        if [ ! -s \$datafile_hdr ]; then
            echo "Error: Invalid hdr file \'\${datafile_hdr}\'. Skipping file."
        else
            new_datafile_hdr="\${datafile_hdr%%.hdr}_updated.hdr"
            cat \$datafile_hdr | tr -c -d '[:print:]\\n\\t' > \$new_datafile_hdr
            printf "NPOL 2\\n" >> \$new_datafile_hdr
            datafile_vdif=\${datafile_hdr%%.hdr}.vdif
            if [ ! -s \$datafile_vdif ]; then
                echo "Error: Invalid vdif file \'\${datafile_vdif}\'. Skipping file."
            else
                size_mb=4096
                outfile=\${datafile_hdr%.hdr}
                dspsr \\
                    -E ${par_file} \\
                    -b \$nbin \\
                    -U \$size_mb \\
                    -F ${fine_chan}:D \\
                    -L ${tint} -A \\
                    -O \$outfile \\
                    \$new_datafile_hdr
            fi
        fi
    done

    # Make a list of channel archives to delete
    find *.ar | xargs -n1 basename > channel_archives.txt

    # The name of the combined archive
    base_name=${psr}_bins\${nbin}_fchans${fine_chan}_tint${tint}

    # Stitch together channels and delete individual channel archives
    psradd -R -o \${base_name}.ar *.ar
    cat channel_archives.txt | xargs rm
    rm channel_archives.txt

    # Flag first time integration
    paz -s 0 -m \${base_name}.ar

    # Plotting
    pav -FTp -C -Dd -g \${base_name}_pulse_profile.png/png \${base_name}.ar
    pav -Tp -C -Gd -g \${base_name}_frequency_phase.png/png \${base_name}.ar
    pav -Fp -C -Yd -g \${base_name}_time_phase.png/png \${base_name}.ar

    dataproduct_dir=${source_dir}/vdif_${duration}s
    if [[ ! -d \${dataproduct_dir}/dspsr ]]; then
        mkdir -p -m 771 \${dataproduct_dir}/dspsr
    fi

    # Move files to publish directory
    mv *_updated.hdr \${dataproduct_dir}
    mv *.ar *.png \${dataproduct_dir}/dspsr
    cp -L -t \${dataproduct_dir}/dspsr *.par

    # If there are beamformed files already, we are re-folding, so skip this step
    old_files=\$(find \$dataproduct_dir -type f -name "*.vdif")
    if [[ -z \$old_files ]]; then
        # Copy VDIF/HDR files into the publish directory and delete from the work directory
        cat vdiffiles.txt | xargs -n1 cp -L -t \$dataproduct_dir
        cat vdiffiles.txt | xargs -n1 readlink -f | xargs -n1 rm
        cat headers.txt | xargs -n1 cp -L -t \$dataproduct_dir
        cat headers.txt | xargs -n1 readlink -f | xargs -n1 rm
    fi
    """
}

process prepfold {
    label 'cpu'
    label 'psrsearch'
    label 'prepfold'

    tag "${psr}"

    time 3.hour

    errorStrategy { task.attempt == 1 ? 'retry' : 'ignore' }
    maxRetries 1

    input:
    val(psr)
    val(source_dir)
    val(duration)
    val(nbin)
    val(nsub)
    val(npart)
    val(nosearch)
    path(vcsbeam_files)
    path(par_file)

    script:
    """
    find *.fits | sort > fitsfiles.txt

    bin_flag=""
    if [[ ! -z \$(grep BINARY ${par_file}) ]]; then
        bin_flag="-bin"
    fi

    nosearch_flag=""
    if [[ "${nosearch}" == "true" ]]; then
        nosearch_flag="-nosearch"
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
    elif (( \$(echo "\$spin_period_ms < ${nbin}/20" | bc -l) )); then
        # Set nbins to 20x the period in ms, and always round down
        nbin=\$(printf "%.0f" \$(echo "scale=0; 20 * \$spin_period_ms - 0.5" | bc))
    else
        nbin=${nbin}
    fi

    prepfold \\
        -ncpus ${task.cpus} \\
        \$par_input \\
        -noxwin \\
        -noclip \\
        -noscales \\
        -nooffsets \\
        -n \$nbin \\
        -nsub ${nsub} \\
        -npart ${npart} \\
        \$bin_flag \\
        \$nosearch_flag \\
        \$(cat fitsfiles.txt)

    dataproduct_dir=${source_dir}/psrfits_${duration}s
    if [[ ! -d \${dataproduct_dir}/prepfold ]]; then
        mkdir -p -m 771 \${dataproduct_dir}/prepfold
    fi

    # Move files to publish directory
    mv *pfd* \${dataproduct_dir}/prepfold
    cp -L -t \${dataproduct_dir}/prepfold *.par

    # If there are beamformed files already, we are re-folding, so skip this step
    old_files=\$(find \$dataproduct_dir -type f -name "*.fits")
    if [[ -z \$old_files ]]; then
        # Copy FITS files into the publish directory and delete from the work directory
        cat fitsfiles.txt | xargs -n1 cp -L -t \$dataproduct_dir
        cat fitsfiles.txt | xargs -n1 readlink -f | xargs -n1 rm
    fi
    """
}

process pdmp {
    label 'cpu'
    label 'psranalysis'

    tag "${psr}"

    time { 2.hour * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'ignore' }
    maxRetries 1

    publishDir "${source_dir}/vdif_${duration}s/dspsr/pdmp", mode: 'move'

    input:
    val(ready)
    val(psr)
    val(source_dir)
    val(duration)
    val(pdmp_mc)
    val(pdmp_ms)

    output:
    path('*.png')
    path('pdmp*')
    path('*bestP0DM')

    script:
    """
    base_dir="${source_dir}/vdif_${duration}s/dspsr"
    find \$base_dir -type f -name "*.ar" -exec ln -s '{}' \\;
    ar_file=\$(find *.ar)
    if [[ \$(echo \$ar_file | wc -l) -gt 1 ]]; then
        echo "Error: More than one archive file found."
    fi

    nchan=\$(psredit -Qc nchan \$ar_file | awk '{print \$2}')
    nsubint=\$(psredit -Qc nsubint \$ar_file | awk '{print \$2}')

    mc_flag=""
    if [ ${pdmp_mc} -lt \$nchan -a \$((\$nchan % ${pdmp_mc})) -eq 0 ]; then
        mc_flag="-mc ${pdmp_mc}"
    fi

    ms_flag=""
    if [ ${pdmp_ms} -lt \$nsubint -a \$((\$nsubint % ${pdmp_ms})) -eq 0 ]; then
        ms_flag="-ms ${pdmp_ms}"
    fi

    pdmp \\
        \$mc_flag \\
        \$ms_flag \\
        -g \${ar_file%.ar}_pdmp.png/png \\
        \${ar_file} \\
        | tee pdmp.log

    # Update the period and DM
    best_p0_ms=\$(grep "Best TC Period" pdmp.log | awk '{print \$6}')
    best_p0_s=\$(printf "%.10f" \$(echo "scale=10; \$best_p0_ms / 1000" | bc))
    best_DM=\$(grep "Best DM" pdmp.log | awk '{print \$4}')
    pam -e bestP0DM --period \$best_p0_s -d \$best_DM \${ar_file}

    # Create the publish directory
    mkdir -p -m 771 \${base_dir}/pdmp
    """
}

process create_tarball {
    label 'cpu'
    label 'tar'

    tag "${psr}"

    time 1.hour

    input:
    val(psr)
    path(vcsbeam_files)

    output:
    path('*.tar')

    script:
    """
    dir_name="${psr}"
    mkdir -p "\$dir_name"

    cp -t "\$dir_name" *.vdif
    cp -t "\$dir_name" *.hdr

    # Follow symlinks and archive
    tar -cvhf "\${PWD}/\${dir_name}.tar" "\$dir_name"

    # Follow links and delete vdif
    find \$PWD -mindepth 1 -maxdepth 1 -name "*.vdif" | xargs -n1 readlink -f | xargs -n1 rm
    """
}

// Script courtesy of Bradley Meyers
process copy_to_acacia {
    label 'copy'

    tag "${psr}"

    shell '/bin/bash', '-veu'
    time 2.hour

    // Nextflow doesn't see the Setonix job in the queue, so will exit
    // However, Setonix job will complete, so ignore error
    errorStrategy 'ignore'

    input:
    val(psr)
    path(tar_file)

    script:
    """
    # Defining variables that will hold the names related to your access, buckets and objects to be stored in Acacia
    profileName="${params.acacia_profile}"
    bucketName="${params.acacia_bucket}"
    prefixPath="${params.acacia_prefix_base}/${params.obsid}"
    fullPathInAcacia="\${profileName}:\${bucketName}/\${prefixPath}"  # Note the colon(:) when using rclone

    # Local storage variables
    tarFileOrigin=\$(find \$PWD -name "*.tar" | xargs -n1 readlink -f)
    workingDir=\$(dirname \$tarFileOrigin)
    tarFileNames=( \$(basename \$tarFileOrigin) )

    #----------------
    # Check if Acacia definitions make sense, and if you can transfer objects into the desired bucket
    echo "Checking that the profile exists"
    rclone config show | grep "\${profileName}" > /dev/null; exitcode=\$?
    if [ \$exitcode -ne 0 ]; then
        echo "The given profileName=\$profileName seems not to exist in the user configuration of rclone"
        echo "Exiting the script with non-zero code in order to inform job dependencies not to continue."
        exit 1
    fi
    echo "Checking the bucket exists and that you have writing access"
    rclone lsd "\${profileName}:\${bucketName}" > /dev/null; exitcode=\$?  # Note the colon(:) when using rclone
    if [ \$exitcode -ne 0 ]; then
        echo "The bucket intended to receive the data does not exist: \${profileName}:\${bucketName}"
        echo "Trying to create it"
        rclone mkdir "\${profileName}:\${bucketName}"; exitcode=\$?
        if [ \$exitcode -ne 0 ]; then
            echo "Creation of bucket failed"
            echo "The bucket name or the profile name may be wrong: \${profileName}:\${bucketName}"
            echo "Exiting the script with non-zero code in order to inform job dependencies not to continue."
            exit 1
        fi
    fi
    echo "Checking if a test file can be trasferred into the desired full path in Acacia"
    testFile=test_file_\${SLURM_JOBID}.txt
    echo "File for test" > "\${testFile}"
    rclone copy "\${testFile}" "\${fullPathInAcacia}/"; exitcode=\$?
    if [ \$exitcode -ne 0 ]; then
        echo "The test file \$testFile cannot be transferred into \${fullPathInAcacia}"
        echo "Exiting the script with non-zero code in order to inform job dependencies not to continue."
        exit 1
    fi
    echo "Checking if the test file can be listed in Acacia"
    listResult=\$(rclone lsl "\${fullPathInAcacia}/\${testFile}")
    if [ -z "\$listResult" ]; then
        echo "Problems occurred during the listing of the test file \${testFile} in \${fullPathInAcacia}"
        echo "Exiting the script with non-zero code in order to inform job dependencies not to continue."
        exit 1
    fi
    echo "Removing test file from Acacia"
    rclone delete "\${fullPathInAcacia}/\${testFile}"; exitcode=\$?
    if [ \$exitcode -ne 0 ]; then
        echo "The test file \$testFile cannot be removed from \${fullPathInAcacia}"
        echo "Exiting the script with non-zero code in order to inform job dependencies not to continue."
        exit 1
    fi
    rm \$testFile
    
    # ----------------
    # Defining the working dir and cd into it
    echo "Checking that the working directory exists"
    if ! [ -d \$workingDir ]; then
        echo "The working directory \$workingDir does not exist"
        echo "Exiting the script with non-zero code in order to inform job dependencies not to continue."
        exit 1
    else
        cd \$workingDir
    fi
    
    #-----------------
    # Perform the transfer of the tar file into the working directory and check for the transfer
    echo "Performing the transfer ... "
    for tarFile in "\${tarFileNames[@]}";do
        echo "rclone sync -P --transfers \${SLURM_CPUS_PER_TASK} --checkers \${SLURM_CPUS_PER_TASK} \${workingDir}/\${tarFile} \${fullPathInAcacia}/ &"
        srun rclone sync -P --transfers \${SLURM_CPUS_PER_TASK} --checkers \${SLURM_CPUS_PER_TASK} "\${workingDir}/\${tarFile}" "\${fullPathInAcacia}/" &
        wait \$!; exitcode=\$?
        if [ \$exitcode -ne 0 ]; then
            echo "Problems occurred during the transfer of file \${tarFile}"
            echo "Check that the file exists in \${workingDir}"
            echo "And that nothing is wrong with the fullPathInAcacia: \${fullPathInAcacia}/"
            echo "Exiting the script with non-zero code in order to inform job dependencies not to continue."
            exit 1
        else
            echo "Final place in Acacia: \${fullPathInAcacia}/\${tarFile}"
        fi
    done

    echo "Done"
    exit 0
    """
}
