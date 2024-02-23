process CHECK_OBS_DIRECTORY {
    tag "${obsid}"

    input:
    val(ready)
    val(base_dir)
    val(obsid)

    output:
    env(data_dir), emit: data_dir
    env(pointings_dir), emit: pointings_dir

    script:
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
    """
}
