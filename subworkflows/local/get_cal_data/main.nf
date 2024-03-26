//
// Retrieve MWA visibility observation data from ASVO
//
// If ASVO job IDs are provided, will move the data into the standard directory
// structure; otherwise, if obs IDs are provided, will download from ASVO before
// moving the data.
//

include { ASVO_VIS_DOWNLOAD    } from '../../../modules/local/asvo_vis_download'
include { CHECK_ASVO_JOB_FILES } from '../../../modules/local/check_asvo_job_files'
include { CHECK_OBSID          } from '../../../modules/local/check_obsid'
include { MOVE_DATA            } from '../../../modules/local/move_data'

workflow GET_CAL_DATA {
    take:
    obsid        //   integer: VCS obs ID
    asvo_job_id  //   integer: job ID for VCS obs download
    asvo_dir     // directory: group ASVO directory
    vcs_dir      // directory: user VCS directory

    main:

    if (asvo_job_id != null) {
        Channel
            .from(asvo_job_id)
            .set { jobid }
    } else {
        ASVO_VIS_DOWNLOAD ( obsid )
            .out
            .jobid
            .set { jobid }
    }

    // Check that data and metadata exist in the ASVO download directories
    CHECK_ASVO_JOB_FILES (
        jobid,
        asvo_dir
    )

    // Check that the obs ID(s) inferred from the metafits are valid
    CHECK_OBSID ( CHECK_ASVO_JOB_FILES.out )
        .map { it -> [it, "none"]}
        .set { obsid_tuple }

    // Move the data into the standard directory structure
    MOVE_DATA (
        jobid,
        vcs_dir,
        asvo_dir,
        obsid,
        'vis',
        obsid_tuple
    ).set { ready }

    emit:
    ready // channel: val(true)
}
