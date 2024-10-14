//
// Retrieve MWA VCS observation data from ASVO
//
// If ASVO job IDs are provided, will move the data into the standard directory
// structure; otherwise, if obs IDs are provided, will download from ASVO before
// moving the data.
//

include { ASVO_VCS_DOWNLOAD    } from '../../modules/local/asvo_vcs_download'
include { CHECK_ASVO_JOB_FILES } from '../../modules/local/check_asvo_job_files'
include { CHECK_OBSID          } from '../../modules/local/check_obsid'
include { CHECK_DATA_FORMAT    } from '../../modules/local/check_data_format'
include { MOVE_VCS_DATA            } from '../../modules/local/move_vcs_data'

workflow GET_VCS_DATA {
    take:
    obsid        //   integer: VCS obs ID
    offset       //   integer: offset from start of observation in seconds
    duration     //   integer: length of time to download in seconds
    vcs_dir      // directory: user VCS directory
    num_dl_jobs  //   integer: number of jobs to split download into

    main:

    ASVO_VCS_DOWNLOAD (
        obsid,
        offset,
        duration,
        num_dl_jobs
    )

    asvo_job_info = ASVO_VCS_DOWNLOAD.out
        .splitCsv()

    // Check that data and metadata exist in the ASVO download directories
    CHECK_ASVO_JOB_FILES (
        asvo_job_info
    )

    // Decide whether the data is raw or combined
    CHECK_DATA_FORMAT ( obsid )

    data_format = CHECK_DATA_FORMAT.out

    // Move the data into the standard directory structure
    MOVE_VCS_DATA (
        asvo_job_info,
        data_format,
        obsid,
        vcs_dir
    )

    ready = MOVE_VCS_DATA.out
        .collect()
        .map { it[0] }

    emit:
    ready // channel: val(true)
}
