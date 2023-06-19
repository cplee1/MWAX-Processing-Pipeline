#!/usr/bin/env nextflow

bf_out = ' -v '
if ( params.fits ) {
    bf_out = ' -p '
}

process get_pointings {
    label 'psranalysis'

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

    RAJ=\$(psrcat -e2 ${psr} | grep "RAJ " | awk '{print \$2}')
    DECJ=\$(psrcat -e2 ${psr} | grep "DECJ " | awk '{print \$2}')
    POINTING="\${RAJ} \${DECJ}"

    if [[ -z \$RAJ || -z \$DECJ ]]; then
        echo "Error: Could not retrieve pointing from psrcat."
        exit 1
    fi

    echo \$POINTING | tee pointings.txt
    echo "${params.flagged_tiles.split(',').join(' ')}" | tee flagged_tiles.txt
    """
}

process vcsbeam {
    label 'gpu'
    label 'vcsbeam'

    time { 1.hour * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    maxRetries 2
    publishDir "${psr_dir}", mode: 'copy'

    input:
    val obsid
    val calid
    tuple val(psr), val(psr_dir), val(pointings), val(flagged_tiles)

    output:
    tuple val(psr), val(psr_dir), path('*.{vdif,hdr,fits}')

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

    srun -N 24 -n 24 make_mwa_tied_array_beam \
        -m ${params.vcs_dir}/${obsid}/${obsid}.metafits \
        -b ${params.startgps} \
        -T ${params.duration} \
        -f ${params.low_chan} \
        -d ${params.vcs_dir}/${obsid}/combined \
        -P ${pointings} \
        -F ${flagged_tiles} \
        -c ${params.vcs_dir}/${obsid}/cal/${calid}/${calid}.metafits \
        -C ${params.vcs_dir}/${obsid}/cal/${calid}/hyperdrive/hyperdrive_solutions.bin \
        -R NONE -U 0,0 -O -X --smart ${bf_out}
    """
}

process dspsr {
    label 'cpu'
    label 'psranalysis'
    label 'dspsr'

    time { 1.hour * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    maxRetries 2
    publishDir "${psr_dir}/dspsr", mode: 'copy'

    input:
    tuple val(psr), val(psr_dir), path(vcsbeam_files)

    output:
    path "*.ar"

    when:
    params.fits == false

    script:
    """
    set -eux
    
    psrname=${psr}
    nbin=${params.nbin}
    nchan=${params.nchan}
    tint=${params.tint}

    find *.hdr | xargs -n1 basename > headers.txt

    f_eph=\${psrname}.eph
    if [ ! -f \${f_eph} ]; then
        psrcat -e \${psrname} > \${f_eph}
    fi

    nfiles=\$(cat headers.txt | wc -l)
    if [ \${nfiles} -lt 1 ]; then
        echo "Error: No header files found."
        exit 1
    fi

    for datafile_hdr in `awk '{ print \$1 }' headers.txt | paste -s -d ' '`; do
        if [ ! -s \$datafile_hdr ]; then
            echo "Error: Invalid hdr file \'\${datafile_hdr}\'. Skipping file."
        else
            datafile_vdif=\${datafile_hdr%.hdr}.vdif
            if [ ! -s \$datafile_vdif ]; then
                echo "Error: Invalid vdif file \'\${datafile_vdif}\'. Skipping file."
            else
                size_mb=4096
                outfile=\${datafile_hdr%.hdr}
                dspsr -E \$f_eph -b \$nbin -U \$size_mb -F \$nchan:D -L \$tint -A -O \$outfile \$datafile_hdr
            fi
        fi
    done

    psradd -R -o \${psrname}_bins\${nbin}_fchans\${nchan}_tint\${tint}.ar *.ar

    if [[ ! -d ${psr_dir}/dspsr ]]; then
        mkdir -p -m 771 ${psr_dir}/dspsr
    fi
    """
}

// Minimum execution requirements: --obsid --startgps --duration --calids --flagged_tiles --psrs
workflow {
    Channel
        .from( params.psrs.split(',') )
        .map { psr -> [ psr, "${params.vcs_dir}/${params.obsid}/pointings/${psr}" ] }
        .set { psr_info }

    get_pointings(params.obsid, params.calid, psr_info) | vcsbeam | dspsr
}