#!/usr/bin/env nextflow

process fake_pointings {
    tag "${num_beams} beams"

    shell '/bin/bash', '-veuo', 'pipefail'

    input:
    val(num_beams)

    output:
    tuple val(num_beams), path('pointings.txt'), path('flagged_tiles.txt')

    script:
    """
    echo "Forming ${num_beams} pointings"
    echo "00:00:00.00 +00:00:00.00" | tee pointings.txt
    x=1
    while [[ \$x -lt ${num_beams} ]]; do
        if [[ \$x -lt 10 ]]; then
            unique_pos="0\${x}"
        elif [[ \$x -lt 90 ]]; then
            unique_pos="\$x"
        else
            echo "Too many beams."
        fi
        echo "00:00:00.00 +\${unique_pos}:00:00.00" | tee -a pointings.txt
        x=\$(( \$x + 1 ))
    done
    touch flagged_tiles.txt
    """
}

process vcsbeam {
    label 'gpu'
    label 'vcsbeam'

    tag "${num_beams} beams"

    shell '/bin/bash', '-veuo', 'pipefail'

    maxForks 3

    time { 1.hour * task.attempt }

    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    maxRetries 1

    input:
    tuple val(num_beams), val(pointings), val(flagged_tiles)

    output:
    tuple val(num_beams), path('vcsbeam_stdout.log'), path('start_stop_unixtimes.log')

    script:
    """
    if [[ ! -r ${params.vcs_dir}/${params.obsid}/${params.obsid}.metafits || \
        ! -r ${params.vcs_dir}/${params.obsid}/cal/${params.calid}/${params.calid}.metafits || \
        ! -r ${params.vcs_dir}/${params.obsid}/cal/${params.calid}/hyperdrive/hyperdrive_solutions.bin ]]; then
        echo "Error: Cannot locate input files for VCSBeam."
        exit 1
    fi

    make_mwa_tied_array_beam -V
    echo "\$(date): Executing make_mwa_tied_array_beam."
    echo \$(date +%s.%N) > start_stop_unixtimes.log
    srun -N ${params.num_chan} -n ${params.num_chan} make_mwa_tied_array_beam \
        -n 10 \
        -m ${params.vcs_dir}/${params.obsid}/${params.obsid}.metafits \
        -b ${params.begin} \
        -T ${params.duration} \
        -f ${params.low_chan} \
        -d ${params.vcs_dir}/${params.obsid}/combined \
        -P ${pointings} \
        -F ${flagged_tiles} \
        -c ${params.vcs_dir}/${params.obsid}/cal/${params.calid}/${params.calid}.metafits \
        -C ${params.vcs_dir}/${params.obsid}/cal/${params.calid}/hyperdrive/hyperdrive_solutions.bin \
        -R NONE -U 0,0 -O -X --smart -p \
        &> vcsbeam_stdout.log
    echo "\$(date): Finished executing make_mwa_tied_array_beam."
    echo \$(date +%s.%N) >> start_stop_unixtimes.log
    """
}

process compute_beamformer_benchmarks {
    beforeScript = "module use ${params.module_dir}; module load python/3.8.2"

    input:
    tuple val(num_beams), path(vcsbeam_log), path(unixtime_log)

    output:
    path('result.txt')

    script:
    """
    #!/usr/bin/env python

    import numpy as np

    with open('${unixtime_log}', 'r') as log_file:
        lines = log_file.readlines()
    execution_time = float(lines[1]) - float(lines[0])

    read_times = []
    upload_times = []
    pfb_times = []
    pfb_wola_times = []
    pfb_round_times = []
    pfb_fft_times = []
    pfb_pack_times = []
    delay_times = []
    calc_times = []
    splice_times = []
    write_times = []
    with open('${vcsbeam_log}', 'r') as log_file:
        lines = log_file.readlines()
        for line in lines:
            if 'Stopwatch read' in line:
                read_times.append(float(line.split(' ')[9]))
            elif 'Stopwatch upload' in line:
                upload_times.append(float(line.split(' ')[7]))
            elif 'Stopwatch pfb ' in line:
                pfb_times.append(float(line.split(' ')[10]))
            elif 'Stopwatch pfb-wola' in line:
                pfb_wola_times.append(float(line.split(' ')[5]))
            elif 'Stopwatch pfb-round' in line:
                pfb_round_times.append(float(line.split(' ')[4]))
            elif 'Stopwatch pfb-fft' in line:
                pfb_fft_times.append(float(line.split(' ')[6]))
            elif 'Stopwatch pfb-pack' in line:
                pfb_pack_times.append(float(line.split(' ')[5]))
            elif 'Stopwatch delay' in line:
                delay_times.append(float(line.split(' ')[8]))
            elif 'Stopwatch calc' in line:
                calc_times.append(float(line.split(' ')[9]))
            elif 'Stopwatch splice' in line:
                splice_times.append(float(line.split(' ')[7]))
            elif 'Stopwatch write' in line:
                write_times.append(float(line.split(' ')[8]))
    ch_avg_read = np.mean(read_times)
    ch_avg_upload = np.mean(upload_times)
    ch_avg_pfb = np.mean(pfb_times)
    ch_avg_pfb_wola = np.mean(pfb_wola_times)
    ch_avg_pfb_round = np.mean(pfb_round_times)
    ch_avg_pfb_fft = np.mean(pfb_fft_times)
    ch_avg_pfb_pack = np.mean(pfb_pack_times)
    ch_avg_delay = np.mean(delay_times)
    ch_avg_calc = np.mean(calc_times)
    ch_avg_splice = np.mean(splice_times)
    ch_avg_write = np.mean(write_times)
    ch_avg_other = ch_avg_upload + ch_avg_pfb + ch_avg_pfb_wola + ch_avg_pfb_round + \
        ch_avg_pfb_fft + ch_avg_pfb_pack + ch_avg_delay + ch_avg_calc + ch_avg_splice

    num_beams = int(${num_beams})
    obs_duration = int(${params.duration})
    with open('result.txt', 'w') as out_file:
        # out_file.write('# duration nbeams exec_time read_time write_time other_time\\n')
        out_file.write(f'{obs_duration} {num_beams} {execution_time:.3f} {ch_avg_read:.3f} {ch_avg_write:.3f} {ch_avg_other:.3f}\\n')
    """
}

process gather_results {
    debug true

    input:
    val(results)

    script:
    """
    eval "results=(\$(echo ${results} | sed 's/\\[//;s/\\]//;s/,/ /g'))"
    echo "duration nbeams exec_time read_time write_time other_time"
    for (( i=0; i<\${#results[@]}; i++ )); do
        cat \${results[i]}
    done
    """
}

workflow bench_bf {
    Channel.from( 1, 2, 4, 8, 16, 32 )  // Number of beams
        | fake_pointings
        | vcsbeam
        | compute_beamformer_benchmarks
        | collect
        | gather_results
}

workflow {
    if ( ! params.obsid ) {
        println "Please provide the obs ID with --obsid."
    } else if ( ! params.duration ) {
        println "Please provide the duration with --duration."
    } else if ( ! params.begin ) {
        println "Please provide the begin time of the observation with --begin."
    } else if ( ! params.calid ) {
        println "Please provide the obs ID of a calibrator observation with --calid."
    } else {
        bench_bf()  // Benchmark the beamformer
    }
}