// Processes
include { asvo_vcs_download      } from '../modules/download_module'
include { asvo_vis_download      } from '../modules/download_module'
include { check_asvo_job_files   } from '../modules/download_module'
include { check_obsid            } from '../modules/download_module'
include { move_data              } from '../modules/download_module'

def skip_vcs_download = params.asvo_id_obs != null ? true : false
def skip_vis_download = params.asvo_id_cals != null ? true : false
def skip_all_download = params.asvo_id_obs != null && params.asvo_id_cals != null ? true : false
def do_all_download = params.asvo_id_obs == null && params.asvo_id_cals == null ? true : false

workflow get_data {
    take:
    obsid
    calids
    asvo_id_obs
    asvo_id_cals
    asvo_dir
    vcs_dir

    main:
    if (skip_all_download || do_all_download) {
        if (skip_all_download) {
            Channel
                .from(asvo_id_obs)
                .map { [ jobid, "${asvo_dir}/${jobid}", 'vcs' ] }
                .set { vcs_job_info }
            Channel
                .from(asvo_id_cals)
                .map { it instanceof String && it.matches('\\w+(\\s\\w+)+') ? it.split('\\s') : it }
                .flatten()
                .map { [ jobid, "${asvo_dir}/${jobid}", 'vis' ] }
                .concat(vcs_job_info)
                .set { job_info }
        } else if (do_all_download) {
            Channel
                .from(calids)
                .map { it instanceof String && it.matches('\\w+(\\s\\w+)+') ? it.split('\\s') : it }
                .flatten()
                .set { calids }
            asvo_vis_download (
                calids
            )
            asvo_vcs_download (
                obsid
            )
            asvo_vis_download
                .out
                .concat(asvo_vcs_download.out)
                .set { job_info }
        }
    } else {
        if (skip_vcs_download) {
            Channel
                .from(asvo_id_obs)
                .map { [ jobid, "${asvo_dir}/${jobid}", 'vcs' ] }
                .set { job_info_vcs }
        } else {
            asvo_vcs_download (
                obsid
            )
            .set { job_info_vcs }
        }
        if (skip_vis_download) {
            Channel
                .from(asvo_id_cals)
                .map { it instanceof String && it.matches('\\w+(\\s\\w+)+') ? it.split('\\s') : it }
                .flatten()
                .map { [ jobid, "${asvo_dir}/${jobid}", 'vis' ] }
                .set { job_info_vis }
        } else {
            Channel
                .from(calids)
                .map { it instanceof String && it.matches('\\w+(\\s\\w+)+') ? it.split('\\s') : it }
                .flatten()
                .set { calids }
            asvo_vis_download (
                calids
            )
            .set { job_info_vis }
        }
        job_info_vcs
            .concat(job_info_vis)
            .set { job_info }
    }
    check_asvo_job_files (
        job_info.map { it[0] },
        job_info.map { it[1] }
    )
    check_obsid (
        check_asvo_job_files.out
    )
    move_data (
        check_obsid.out,
        vcs_dir,
        job_info.map { it[1] },
        obsid,
        check_asvo_job_files.out,
        job_info.map { it[2] },
    )
    .set { ready }

    emit:
    ready
}
