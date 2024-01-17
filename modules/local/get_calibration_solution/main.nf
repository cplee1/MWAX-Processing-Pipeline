process GET_CALIBRATION_SOLUTION {
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
