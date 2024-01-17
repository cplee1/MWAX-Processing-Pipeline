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
