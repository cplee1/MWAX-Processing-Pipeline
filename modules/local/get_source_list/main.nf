process GET_SOURCE_LIST {
    label 'cpu'
    label 'srclist'

    time { 5.minute * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    maxRetries 4
    publishDir "${cal_dir}/hyperdrive", mode: 'copy'

    input:
    tuple val(calid), val(cal_dir), val(source), val(flagged_tiles), val(flagged_fine_chans), val(metafits)

    output:
    tuple val(calid), val(cal_dir), val(flagged_tiles), val(flagged_fine_chans), val(metafits), path('srclist.yaml')

    script:
    """
    if [[ ! -r ${params.src_catalogue} ]]; then
        echo "Error: Source catalogue cannot be found."
        exit 1
    fi

    echo "Creating list of 1000 brightest sources from catalogue."
    srclist=srclist_1000.yaml
    hyperdrive srclist-by-beam \\
        --metafits ${metafits} \\
        --number 1000 \\
        ${params.src_catalogue} \\
        \$srclist

    echo "Looking for specific source model."
    specific_model=\$(grep ${source} ${projectDir}/../source_lists.txt | awk '{print \$2}')
    if [[ -z \$specific_model ]]; then
        echo "No specific model found in lookup table."
        echo "Using catalogue sources."
    elif [[ ! -r ${params.models_dir}/\$specific_model ]]; then
        echo "Specific model found in lookup table does not exist: ${params.models_dir}/\${specific_model}"
        echo "Using catalogue sources."
    else
        echo "Specific model found: ${params.models_dir}/\${specific_model}"
        echo "Converting model to yaml format."
        srclist=srclist_specific.yaml
        hyperdrive srclist-by-beam \\
            --metafits ${metafits} \\
            --number 1 \\
            ${params.models_dir}/\$specific_model \\
            \$srclist
        echo "Using specific source model."
    fi

    mv \$srclist srclist.yaml
    """
}
