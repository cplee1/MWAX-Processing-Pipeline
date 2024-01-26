//
// Calibrate observations using Birli and Hyperdrive
//

workflow cal {
    take:
        obsid
    main:
        Channel.from( params.calibrators.split(' ') )
            | map { calibrator -> [ calibrator.split(':')[0], "${params.vcs_dir}/${obsid}/cal/${calibrator.split(':')[0]}", calibrator.split(':')[1], params.flagged_tiles, params.flagged_fine_chans ] }
            | set { cal_info }

        if ( params.skip_birli ) {
            check_cal_directory(cal_info)
                | get_source_list
                | hyperdrive
                | map { it[1] }
                | set { cal_dirs }
        } else {
            check_cal_directory(cal_info)
                | birli
                | get_source_list
                | hyperdrive
                | map { it[1] }
                | set { cal_dirs }
        }
    emit:
        cal_dirs
}

workflow cal_jobs {
    take:
        joblist
    main:
        // Job list inputs:
        // cal_dir,calid,source,flagged_tiles,flagged_fine_chans
        Channel.fromPath( joblist )
            | splitCsv
            | check_cal_directory
            | get_source_list
            | hyperdrive
            | map { it[1] }
            | set { cal_dirs }
    emit:
        cal_dirs
}
