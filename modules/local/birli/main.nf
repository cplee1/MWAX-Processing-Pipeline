process BIRLI {
    label 'cpu'
    label 'birli'

    time { 1.hour * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    maxRetries 1

    input:
    tuple val(calid), val(cal_dir), val(source), val(flagged_tiles), val(flagged_fine_chans), val(metafits)

    output:
    tuple val(calid), val(cal_dir), val(source), val(flagged_tiles), val(flagged_fine_chans), val(metafits)

    script:
    """
    if [[ -r ${cal_dir}/${calid}_birli.uvfits && ${params.force_birli} == 'false' ]]; then
        echo "Birli files found. Skipping process."
        exit 0
    fi

    birli -V
    birli \\
        --metafits ${metafits} \\
        --avg-time-res ${params.dt} \\
        --avg-freq-res ${params.df} \\
        --uvfits-out ${calid}_birli.uvfits \\
        ${cal_dir}/*ch???*.fits

    cp ${calid}_birli.uvfits ${cal_dir}/${calid}_birli.uvfits
    """
}
