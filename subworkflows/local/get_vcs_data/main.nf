//
// Retrieve MWA VCS observation data from ASVO
//
// If ASVO job IDs are provided, will move the data into the standard directory
// structure; otherwise, if obs IDs are provided, will download from ASVO before
// moving the data.
//

include { ASVO_VCS_DOWNLOAD    } from '../../../modules/local/asvo_vcs_download'
include { CHECK_ASVO_JOB_FILES } from '../../../modules/local/check_asvo_job_files'
include { CHECK_OBSID          } from '../../../modules/local/check_obsid'
include { CHECK_DATA_FORMAT    } from '../../../modules/local/check_data_format'
include { MOVE_DATA            } from '../../../modules/local/move_data'

workflow GET_VCS_DATA {
    take:
    obsid        //   integer: VCS obs ID
    offset       //   integer: offset from start of observation in seconds
    duration     //   integer: length of time to download in seconds
    asvo_job_id  //   integer: job ID for VCS obs download
    asvo_dir     // directory: group ASVO directory
    vcs_dir      // directory: user VCS directory

    main:

    if (asvo_job_id != null) {
        jobid = Channel.from(asvo_job_id)
    } else {
        ASVO_VCS_DOWNLOAD (
            obsid,
            offset,
            duration
        )
        jobid = ASVO_VCS_DOWNLOAD.out.jobid
    }

    // Check that data and metadata exist in the ASVO download directories
    CHECK_ASVO_JOB_FILES (
        jobid,
        asvo_dir
    )

    // Check that the obs ID(s) inferred from the metafits are valid
    CHECK_OBSID ( CHECK_ASVO_JOB_FILES.out )

    // Decide whether the data is raw or combined
    CHECK_DATA_FORMAT ( CHECK_OBSID.out )

    // Move the data into the standard directory structure
    MOVE_DATA (
        jobid,
        vcs_dir,
        asvo_dir,
        obsid,
        'vcs',
        CHECK_DATA_FORMAT.out
    )

    emit:
    MOVE_DATA.out.ready // channel: val(true)
}
