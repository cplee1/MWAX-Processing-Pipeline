process BIRLI {
    label 'cpu'
    label 'birli'

    time { 1.hour * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    maxRetries 1
    publishDir "${cal_dir}/${calid}", mode: 'copy'

    input:
    val(ready)
    val(calid)
    val(cal_dir)
    val(dt)
    val(df)

    output:
    val(true), emit:ready
    path("${calid}_birli*.uvfits"), emit: uvfits

    script:
    """
    birli -V
    birli \\
        --metafits ${cal_dir}/${calid}/${calid}.metafits \\
        --avg-time-res ${dt} \\
        --avg-freq-res ${df} \\
        --uvfits-out ${calid}_birli.uvfits \\
        ${cal_dir}/${calid}/*ch???*.fits
    """
}
