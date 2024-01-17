//
// Retrieve MWA observation data from ASVO
//
// If ASVO job IDs are provided, will move the data into the standard directory
// structure; otherwise, if obs IDs are provided, will download from ASVO before
// moving the data.
//

include { ASVO_VCS_DOWNLOAD    } from '../../../modules/local/asvo_vcs_download'
include { ASVO_VIS_DOWNLOAD    } from '../../../modules/local/asvo_vis_download'
include { CHECK_ASVO_JOB_FILES } from '../../../modules/local/check_asvo_job_files'
include { CHECK_OBSID          } from '../../../modules/local/check_obsid'
include { MOVE_DATA            } from '../../../modules/local/move_data'

workflow GET_MWA_DATA {
    take:
    obsid        //   integer: VCS obs ID
    calids       //    string: space separated list of calibrator obs IDs
    asvo_id_obs  //   integer: job ID for VCS obs download
    asvo_id_cals //    string: space separated list of job IDs for cal downloads
    asvo_dir     // directory: group ASVO directory
    vcs_dir      // directory: user VCS directory

    main:

    //
    // Get VCS observation
    //
    if (asvo_id_obs != null) {
        Channel
            .from(asvo_id_obs)
            .map { [ jobid, "${asvo_dir}/${jobid}", 'vcs' ] }
            .set { job_info_vcs }
    } else {
        ASVO_VCS_DOWNLOAD ( obsid )
            .set { job_info_vcs }
    }
    
    //
    // Get imaging observation(s) for calibrating the VCS observation
    //
    if (asvo_id_cals != null) {
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
        ASVO_VIS_DOWNLOAD ( calids )
            .set { job_info_vis }
    }

    job_info_vcs
        .concat(job_info_vis)
        .set { job_info }

    //
    // Check that data and metadata exist in the ASVO download directories
    //
    CHECK_ASVO_JOB_FILES (
        job_info.map { it[0] },
        job_info.map { it[1] }
    )

    //
    // Check that the obs ID(s) inferred from the metafits are valid
    //
    CHECK_OBSID ( CHECK_ASVO_JOB_FILES.out )

    //
    // Move the data into the standard directory structure
    //
    MOVE_DATA (
        CHECK_OBSID.out,
        vcs_dir,
        job_info.map { it[1] },
        obsid,
        CHECK_ASVO_JOB_FILES.out,
        job_info.map { it[2] },
    )
    .set { ready }

    emit:
    ready // channel: val(true)
}
