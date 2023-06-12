#!/usr/bin/env nextflow

process get_pointings {
    label 'psrsearch'

    input:
    val obsid
    val calid
    tuple val(psr), val(psr_dir)

    output:
    val obsid
    val calid
    tuple val(psr), val(psr_dir), path('pointings.txt'), path('flagged_tiles.txt')

    script:
    """
    set -eux

    RAJ=\$(psrcat -e ${psr} | grep RAJ | awk '{print \$2}')
    DECJ=\$(psrcat -e ${psr} | grep DECJ | awk '{print \$2}')
    POINTING="\${RAJ} \${DECJ}"

    echo \$POINTING | tee pointings.txt
    echo "${params.flagged_tiles.split(',').join(' ')}" | tee flagged_tiles.txt
    """
}

process vcsbeam {
    label 'gpu'
    label 'vcsbeam'

    time { 1.hours * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    maxRetries 2
    publishDir "${psr_dir}"

    input:
    val obsid
    val calid
    tuple val(psr), val(psr_dir), val(pointings), val(flagged_tiles)

    output:
    path '*.vdif', emit: voltages
    path '*.hdr', emit: headers

    script:
    """
    set -eux
    which make_mwa_tied_array_beam

    if [[ ! -r ${params.vcs_dir}/${obsid}/${obsid}.metafits || \
          ! -r ${params.vcs_dir}/${obsid}/cal/${calid}/${calid}.metafits || \
          ! -r ${params.vcs_dir}/${obsid}/cal/${calid}/hyperdrive/hyperdrive_solutions.bin ]]; then
        echo "Error: Cannot find files for VCSBeam."
        exit 1
    fi

    if [[ ! -d ${psr_dir} ]]; then
        mkdir -p -m 771 ${psr_dir}
    fi

    make_mwa_tied_array_beam \
        -m ${params.vcs_dir}/${obsid}/${obsid}.metafits \
        -b ${params.startgps} \
        -T ${params.duration} \
        -f ${params.low_chan} \
        -d ${params.vcs_dir}/${obsid}/combined \
        -P ${pointings} \
        -F ${flagged_tiles} \
        -c ${params.vcs_dir}/${obsid}/cal/${calid}/${calid}.metafits \
        -C ${params.vcs_dir}/${obsid}/cal/${calid}/hyperdrive/hyperdrive_solutions.bin \
        -v -R NONE -U 0,0 -O -X --smart
    """
}

// Minimum execution requirements: --obsid --startgps --duration --calids --flagged_tiles --psrs
workflow {
    Channel
        .from( params.psrs.split(',') )
        .map { psr -> [ psr, "${params.vcs_dir}/${params.obsid}/pointings/${psr}" ] }
        .set { psr_info }
    
    Channel
        .from( params.calids.split(',') )
        .first()
        .set { calid }

    get_pointings(params.obsid, calid, psr_info) | vcsbeam
}