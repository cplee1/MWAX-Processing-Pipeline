include { asvo_vcs_download      } from '../modules/download_module'
include { asvo_vis_download      } from '../modules/download_module'
include { check_asvo_job_files   } from '../modules/download_module'
include { check_obsid            } from '../modules/download_module'
include { move_data              } from '../modules/download_module'

workflow mv {
    main:
        if ( params.asvo_id_obs ) {
            Channel.from( params.asvo_id_obs )
                | map { jobid -> [ jobid, "${params.asvo_dir}/${jobid}", 'vcs' ] }
                | check_asvo_job_files
                | check_obsid
                | move_data
                | set { obsid }
        }
        if ( params.asvo_id_cals ) {
            Channel.from( params.asvo_id_cals )
                | map { it.split(' ') }
                | flatten
                | map { jobid -> [ jobid, "${params.asvo_dir}/${jobid}", 'vis' ] }
                | check_asvo_job_files
                | check_obsid
                | move_data
                | set { obsid }
        }

    emit:
        obsid
}

workflow dl {
    main:
        if ( params.obsid ) {
            asvo_vcs_download(params.obsid)
                | check_asvo_job_files
                | check_obsid
                | move_data
                | set { obsid }
        }
        // if ( params.calids ) {
        //     Channel.from( params.calids )
        //         | map { it.split(' ') }
        //         | flatten
        //         | asvo_vis_download
        //         | check_asvo_job_files
        //         | check_obsid
        //         | move_data
        //         | set { obsid }
        // }

    emit:
        obsid
}
