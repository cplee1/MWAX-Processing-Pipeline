process GET_SOURCE_LIST {
    label 'cpu'
    label 'srclist'

    time { 5.minute * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    maxRetries 4
    publishDir "${cal_dir}/hyperdrive", mode: 'copy'

    input:
    val(ready)
    tuple val(calid), val(source)
    val(cal_dir)
    val(src_catalogue)
    val(models_dir)
    
    output:
    path('srclist.yaml')

    script:
    """
    if [[ ! -r ${src_catalogue} ]]; then
        echo "Error: Source catalogue cannot be found."
        exit 1
    fi

    echo "Creating list of 1000 brightest sources from catalogue."
    srclist=srclist_1000.yaml
    hyperdrive srclist-by-beam \\
        --metafits ${metafits} \\
        --number 1000 \\
        ${src_catalogue} \\
        \$srclist

    echo "Looking for specific source model."
    specific_model=\$(grep ${source} ${projectDir}/../source_lists.txt | awk '{print \$2}')
    if [[ -z \$specific_model ]]; then
        echo "No specific model found in lookup table."
        echo "Using catalogue sources."
    elif [[ ! -r ${models_dir}/\$specific_model ]]; then
        echo "Specific model found in lookup table does not exist: ${models_dir}/\${specific_model}"
        echo "Using catalogue sources."
    else
        echo "Specific model found: ${models_dir}/\${specific_model}"
        echo "Converting model to yaml format."
        srclist=srclist_specific.yaml
        hyperdrive srclist-by-beam \\
            --metafits ${cal_dir}/${calid}/${calid}.metafits \\
            --number 1 \\
            ${models_dir}/\$specific_model \\
            \$srclist
        echo "Using specific source model."
    fi

    mv \$srclist srclist.yaml
    """
}
