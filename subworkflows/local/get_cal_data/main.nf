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
include { MOVE_VIS_DATA            } from '../../../modules/local/move_vis_data'

workflow GET_CAL_DATA {
    take:
    obsid        //   integer: VCS obs ID
    vcs_dir      // directory: user VCS directory

    main:

    ASVO_VIS_DOWNLOAD ( obsid )

    asvo_job_info = ASVO_VIS_DOWNLOAD.out
        .splitCsv()

    // Check that data and metadata exist in the ASVO download directories
    CHECK_ASVO_JOB_FILES (
        asvo_job_info
    )

    // Move the data into the standard directory structure
    MOVE_VIS_DATA (
        asvo_job_info,
        obsid,
        vcs_dir
    )

    ready = MOVE_VIS_DATA.out

    emit:
    ready // channel: val(true)
}
