#!/usr/bin/env nextflow

process check_files_exist {
    input:
    val asvo_id_obs
    val asvo_id_cals

    output:
    tuple val(asvo_id_obs), val(asvo_id_cals), env(OBSID), env(CALIDS)

    script:
    """
    if [[ ! -d "${params.asvo_dir}/${asvo_id_obs}" ]]; then
        echo "Error: Voltages ASVO directory does not exist."
        exit 1
    fi
    if [[ \$(find ${params.asvo_dir}/${asvo_id_obs}/*.sub | wc -l) -lt 1 ]]; then
        echo "Error: Cannot locate voltage files."
        exit 1
    fi
    OBSID=\$(find ${params.asvo_dir}/${asvo_id_obs}/*.metafits | xargs -n1 basename -s ".metafits")

    # Turn the Nextflow list into a Bash array
    ASVO_ID_CALS="${asvo_id_cals}"
    ASVO_ID_CALS="\${ASVO_ID_CALS:1:-1}"
    ASVO_ID_CALS="\${ASVO_ID_CALS//,/ }"
    eval "ASVO_ID_CALS=(\$ASVO_ID_CALS)"

    CALIDS=""
    for (( i=0; i<\${#ASVO_ID_CALS[@]}; i++ )); do
        if [[ ! -d "${params.asvo_dir}/\${ASVO_ID_CALS[i]}" ]]; then
            echo "Error: Calibrator ASVO directory does not exist."
            exit 1
        fi
        if [[ \$(find ${params.asvo_dir}/\${ASVO_ID_CALS[i]}/*.metafits | wc -l) != 1 ]]; then
            echo "Error: Cannot locate calibrator metafits file."
            exit 1
        fi
        CALID=\$(find ${params.asvo_dir}/\${ASVO_ID_CALS[i]}/*.metafits | xargs -n1 basename -s ".metafits")
        CALIDS="\${CALIDS},\${CALID}"
    done

    # Remove the leading comma
    CALIDS="\${CALIDS:1}"
    """
}

process get_obsids {
    input:
    tuple val(asvo_id_obs), val(asvo_id_cals), val(obsid), val(calids)

    output:
    tuple val(asvo_id_obs), val(asvo_id_cals), val(obsid), val(calids)

    script:
    """
    #!/usr/bin/env python
    import sys

    def check_obsid(string):
        if string.isdigit() and len(string) == 10:
            return True
        else:
            return False

    if not (check_obsid('${obsid}')): sys.exit(1)

    for calid in "${calids}".split(','):
        if not (check_obsid(calid)): sys.exit(1)
    """
}

process move_download_files {
    debug true

    input:
    tuple val(asvo_id_obs), val(asvo_id_cals), val(obsid), val(calids)

    script:
    """
    if [[ ! -d ${params.vcs_dir} ]]; then
        echo "Error: VCS directory does not exist."
        exit 1
    fi

    mkdir -p -m 771 ${params.vcs_dir}/${obsid}/combined
    mv ${params.asvo_dir}/${asvo_id_obs}/*.sub ${params.vcs_dir}/${obsid}/combined
    mv ${params.asvo_dir}/${asvo_id_obs}/*.metafits ${params.vcs_dir}/${obsid}
    
    if [[ -d ${params.asvo_dir}/${asvo_id_obs} && -z "\$(ls -A ${params.asvo_dir}/${asvo_id_obs})" ]]; then
        rm -r ${params.asvo_dir}/${asvo_id_obs}
    fi

    # Turn the comma separated list into a Bash array
    IFS=',' read -ra CALIDS <<< "${calids}"

    # Turn the nextflow list into a Bash array
    ASVO_ID_CALS="${asvo_id_cals}"
    ASVO_ID_CALS="\${ASVO_ID_CALS:1:-1}"
    ASVO_ID_CALS="\${ASVO_ID_CALS//,/ }"
    eval "ASVO_ID_CALS=(\$ASVO_ID_CALS)"

    for (( i=0; i<\${#CALIDS[@]}; i++ )); do
        mkdir -p -m 771 /astro/mwavcs/cplee/mwax_pipeline_testing/vcs/1370457464/cal/\${CALIDS[i]}/hyperdrive
        mv ${params.asvo_dir}/\${ASVO_ID_CALS[i]}/* ${params.vcs_dir}/${obsid}/cal/\${CALIDS[i]}
    
        if [[ -d ${params.asvo_dir}/\${ASVO_ID_CALS[i]} && -z "\$(ls -A ${params.asvo_dir}/\${ASVO_ID_CALS[i]})" ]]; then
            rm -r ${params.asvo_dir}/\${ASVO_ID_CALS[i]}
        fi
    done
    """
}

workflow {
    Channel
        .from( params.asvo_id_cals.split(',') )
        .set { asvo_id_cals }

    check_files_exist(params.asvo_id_obs, asvo_id_cals.collect()) | get_obsids | move_download_files
}
