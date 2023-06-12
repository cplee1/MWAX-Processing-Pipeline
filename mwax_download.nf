#!/usr/bin/env nextflow

process check_files_exist {
    output:
    tuple env(OBS_METAFITS_PATH), env(CAL1_METAFITS_PATH), env(CAL2_METAFITS_PATH)

    script:
    """
    if [[ ! -d "${params.download_dir_obs}" || \
          ! -d "${params.download_dir_cal1}" || \
          ! -d "${params.download_dir_cal2}" ]]; then
        echo "Error: Directories do no exist."
        exit 1
    fi
    if [[ \$(find ${params.download_dir_obs}/*.sub | wc -l) -lt 1 || \
          \$(find ${params.download_dir_cal1}/*.fits | wc -l) -lt 1 || \
          \$(find ${params.download_dir_cal2}/*.fits | wc -l) -lt 1 ]]; then
        echo "Error: Cannot find data files."
        exit 1
    fi
    if [[ \$(find ${params.download_dir_obs}/*.metafits | wc -l) != 1 || \
          \$(find ${params.download_dir_cal1}/*.metafits | wc -l) != 1 || \
          \$(find ${params.download_dir_cal2}/*.metafits | wc -l) != 1 ]]; then
        echo "Error: Cannot find metafits files."
        exit 1
    fi

    OBS_METAFITS_PATH=\$(find ${params.download_dir_obs}/*.metafits)
    CAL1_METAFITS_PATH=\$(find ${params.download_dir_cal1}/*.metafits)
    CAL2_METAFITS_PATH=\$(find ${params.download_dir_cal2}/*.metafits)
    """
}

process get_obsids {
    input:
    tuple path(obs_metafits), path(cal1_metafits), path(cal2_metafits)

    output:
    path 'obsids.csv'

    script:
    """
    #!/usr/bin/env python
    import sys

    def check_obsid(string):
        if string.isdigit() and len(string) == 10:
            return True
        else:
            return False

    if not (check_obsid('${obs_metafits.baseName}') and \
        check_obsid('${cal1_metafits.baseName}') and \
        check_obsid('${cal1_metafits.baseName}')):
        sys.exit(1)
    
    with open("obsids.csv", "w") as outfile:
        outfile.write(f"{${obs_metafits.baseName}},{${cal1_metafits.baseName}},{${cal2_metafits.baseName}}")
    """
}

process move_download_files {
    input:
    tuple val(obsid), val(calid1), val(calid2)

    script:
    """
    if [[ ! -d ${params.vcs_dir} ]]; then
        echo "Error: Could not find VCS directory."
        exit 1
    fi

    mkdir -p -m 771 ${params.vcs_dir}/${obsid}/combined
    mkdir -p -m 771 ${params.vcs_dir}/${obsid}/cal/${calid1}/hyperdrive
    mkdir -p -m 771 ${params.vcs_dir}/${obsid}/cal/${calid2}/hyperdrive

    mv ${params.download_dir_obs}/*.sub ${params.vcs_dir}/${obsid}/combined
    mv ${params.download_dir_obs}/*.metafits ${params.vcs_dir}/${obsid}
    mv ${params.download_dir_cal1}/* ${params.vcs_dir}/${obsid}/cal/${calid1}
    mv ${params.download_dir_cal2}/* ${params.vcs_dir}/${obsid}/cal/${calid2}
    """
}

workflow {
    check_files_exist | get_obsids | splitCsv | move_download_files
}