#!/usr/bin/env nextflow

/*
    Singlepixel Module
    ~~~~~~~~~~~~~~~~~~
    This module contains processes and workflows for beamforming and folding on 
    multiple pulsars independently of one another. Each pulsar is assigned its
    own beamforming job which is then fed into a folding job.

    Since we use the multipixel beamformer for PSRFITS output, the singlepixel
    beamforming workflow assumes VDIF output. The locate_fits_files and prepfold
    processes are included here for if the use wants to re-fold their PSRFITS
    data, which does not involve the multipixel beamformer.
*/

if ( params.psrs || params.acacia_prefix_base ) {
    publish_vdif = false
} else {
    publish_vdif = true
}

process locate_vdif_files {
    shell '/bin/bash', '-veuo', 'pipefail'

    input:
    val(psr)

    output:
    tuple val(psr), path('*.{vdif,hdr}')

    script:
    """
    psr_dir="${params.vcs_dir}/${params.obsid}/pointings/${psr}/vdif_${params.duration}s"
    if [[ ! -d \$psr_dir ]]; then
        echo "Error: Cannot locate data directory."
        exit 1
    fi
    find \$psr_dir -type f -name "*.vdif" -exec ln -s '{}' \\;
    find \$psr_dir -type f -name "*.hdr" -exec ln -s '{}' \\;

    if [[ -d \${psr_dir}/dspsr ]]; then
        old_files=\$(find \${psr_dir}/dspsr -type f)
        if [[ -n \$old_files ]]; then
            archive="\${psr_dir}/dspsr_archived_\$(date +%s)"
            mkdir -p -m 771 \$archive
            echo \$old_files | xargs -n1 mv -t \$archive
        fi
    fi
    """
}

process locate_fits_files {
    shell '/bin/bash', '-veuo', 'pipefail'

    input:
    val(psr)

    output:
    tuple val(psr), path('*.fits')

    script:
    """
    psr_dir="${params.vcs_dir}/${params.obsid}/pointings/${psr}/psrfits_${params.duration}s"
    if [[ ! -d \$psr_dir ]]; then
        echo "Error: Cannot locate data directory."
        exit 1
    fi
    find \$psr_dir -type f -name "*.fits" -exec ln -s '{}' \\;

    if [[ -d \${psr_dir}/prepfold ]]; then
        old_files=\$(find \${psr_dir}/prepfold -type f)
        if [[ -n \$old_files ]]; then
            archive="\${psr_dir}/prepfold_archived_\$(date +%s)"
            mkdir -p -m 771 \$archive
            echo \$old_files | xargs -n1 mv -t \$archive
        fi
    fi
    """
}

process parse_pointings {
    shell '/bin/bash', '-veuo', 'pipefail'

    input:
    tuple val(RAJ), val(DECJ)

    output:
    tuple env(pointing_label), path('pointings.txt'), path('flagged_tiles.txt')

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

        # Label for naming files and directories
        pointing_label="${RAJ}_${DECJ}"

        # Write equatorial coordinates to file
        echo "${RAJ} ${DECJ}" | tee pointings.txt

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

        # Label for naming files and directories
        pointing_label="${RAJ}_${DECJ}"

        # Write equatorial coordinates to file
        echo "${RAJ} ${DECJ}" | tee pointings.txt

        # Write the tile flags to file
        echo "${params.flagged_tiles}" | tee flagged_tiles.txt
        """
    }
}

process get_pointings {
    label 'psranalysis'

    shell '/bin/bash', '-veuo', 'pipefail'

    input:
    val(psr)

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
    psr_dir="${params.vcs_dir}/${params.obsid}/pointings/${psr}/vdif_${params.duration}s"
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

    # Write the tile flags to file
    echo "${params.flagged_tiles}" | tee flagged_tiles.txt
    """
}

process vcsbeam {
    label 'gpu'
    label 'vcsbeam'

    shell '/bin/bash', '-veuo', 'pipefail'

    maxForks 5

    time { 2.hour * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'finish' }
    maxRetries 1

    publishDir "${params.vcs_dir}/${params.obsid}/pointings/${psr}/vdif_${params.duration}s", mode: 'move', enabled: publish_vdif

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
    tuple val(psr), val(vcsbeam_files)

    output:
    tuple val(psr), val(vcsbeam_files), path("${psr}.par")

    script:
    """
    if [[ -z ${psr} ]]; then
        echo "Error: Pulsar name string is blank."
        exit 2
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
            exit 3
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

    time { 2.hour * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'ignore' }
    maxRetries 2

    input:
    tuple val(psr), path(vcsbeam_files), path(par_file)

    output:
    val(psr)

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
    pav -TpC -Gd -g \${base_name}_frequency_phase.png/png \${base_name}.ar
    pav -FpC -Y -g \${base_name}_time_phase.png/png \${base_name}.ar

    dataproduct_dir=${params.vcs_dir}/${params.obsid}/pointings/${psr}/vdif_${params.duration}s
    if [[ ! -d \${dataproduct_dir}/dspsr ]]; then
        mkdir -p -m 771 \${dataproduct_dir}/dspsr
    fi

    # Move files to publish directory
    mv *.ar *.png \${dataproduct_dir}/dspsr

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

    shell '/bin/bash', '-veuo', 'pipefail'

    time 2.hour

    errorStrategy { task.attempt == 1 ? 'retry' : 'ignore' }
    maxRetries 1

    input:
    tuple val(psr), path(vcsbeam_files), path(par_file)

    script:
    """
    find *.fits | sort > fitsfiles.txt

    bin_flag=""
    if [[ ! -z \$(grep BINARY ${par_file}) ]]; then
        bin_flag="-bin"
    fi

    nosearch_flag=""
    if [[ "${params.nosearch_prepfold}" == "true" ]]; then
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
        \$nosearch_flag \
        \$nosearch_flag \
        \$(cat fitsfiles.txt)

    dataproduct_dir=${params.vcs_dir}/${params.obsid}/pointings/${psr}/psrfits_${params.duration}s
    if [[ ! -d \${dataproduct_dir}/prepfold ]]; then
        mkdir -p -m 771 \${dataproduct_dir}/prepfold
    fi

    # Move files to publish directory
    mv *pfd* \${dataproduct_dir}/prepfold

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
    
    shell '/bin/bash', '-veuo', 'pipefail'

    time { 4.hour * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'ignore' }
    maxRetries 1
    publishDir "${params.vcs_dir}/${params.obsid}/pointings/${psr}/vdif_${params.duration}s/dspsr/pdmp", mode: 'move'

    input:
    val(psr)

    output:
    path('*.png')
    path('pdmp*')

    script:
    """
    base_dir="${params.vcs_dir}/${params.obsid}/pointings/${psr}/vdif_${params.duration}s/dspsr"
    find \$base_dir -type f -name "*.ar" -exec ln -s '{}' \\;
    ar_file=\$(find *.ar)
    if [[ \$(echo \$ar_file | wc -l) -gt 1 ]]; then
        echo "Error: More than one archive file found."
    fi

    nchan=\$(psredit -Qc nchan \$ar_file | awk '{print \$2}')
    nsubint=\$(psredit -Qc nsubint \$ar_file | awk '{print \$2}')

    mc_flag=""
    if [ ${params.pdmp_mc} -lt \$nchan -a \$((\$nchan % ${params.pdmp_mc})) -eq 0 ]; then
        mc_flag="-mc ${params.pdmp_mc}"
    fi

    ms_flag=""
    if [ ${params.pdmp_ms} -lt \$nsubint -a \$((\$nsubint % ${params.pdmp_ms})) -eq 0 ]; then
        ms_flag="-ms ${params.pdmp_ms}"
    fi

    pdmp \
        \$mc_flag \
        \$ms_flag \
        -g \${ar_file%.ar}_pdmp.png/png \
        \${ar_file}

    # Create the publish directory
    mkdir -p -m 771 \${base_dir}/pdmp
    """
}

process create_tarball {
    label 'cpu'
    label 'tar'

    shell '/bin/bash', '-veuo', 'pipefail'

    time 1.hour

    input:
    tuple val(psr), path(vcsbeam_files)

    output:
    tuple val(psr), path('*.tar')

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

    shell '/bin/bash', '-veu'
    time 2.hour

    // Nextflow doesn't see the Setonix job in the queue, so will exit
    // However, Setonix job will complete, so ignore error
    errorStrategy 'ignore'

    input:
    tuple val(psr), path(tar_file)

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

// Beamform and fold on catalogued pulsar in VDIF/singlepixel mode
workflow spsr {
    take:
        // Channel of individual pulsar Jnames
        psrs
    main:
        // Beamform and fold each pulsar
        if ( params.nosearch_pdmp ) {
            get_pointings(psrs)
                | vcsbeam
                | get_ephemeris
                | dspsr
        } else {
            get_pointings(psrs)
                | vcsbeam
                | get_ephemeris
                | dspsr
                | pdmp
        }
}

// Beamform on pointing in VDIF/singlepixel mode
workflow spt {
    take:
        // Channel of individual pointings
        pointings
    main:
        if ( params.acacia_prefix_base ) {
            // Beamform on each pointing and copy to Acacia
            parse_pointings(pointings)
                | vcsbeam
                | create_tarball
                | copy_to_acacia
        } else {
            // Beamform on each pointing
            parse_pointings(pointings)
                | vcsbeam
        }
}

// Fold on catalogued pulsar with dspsr and search with pdmp
workflow dspsr_wf {
    take:
        // Channel of individual pulsar Jnames
        psrs
    main:
        if ( params.nosearch_pdmp ) {
            locate_vdif_files(psrs)
                | get_ephemeris
                | dspsr
        } else {
            locate_vdif_files(psrs)
                | get_ephemeris
                | dspsr
                | pdmp
        }
}

// Fold on catalogued pulsar with prepfold
workflow prepfold_wf {
    take:
        // Channel of individual pulsar Jnames
        psrs
    main:
        locate_fits_files(psrs)
            | get_ephemeris
            | prepfold
}
