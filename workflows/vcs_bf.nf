//
// Download from ASVO, beamform, fold, search, plot, and upload to Acacia
//

include { CHECK_OBS_DIRECTORY      } from '../modules/local/check_obs_directory'
include { CREATE_DATA_DIRECTORIES  } from '../modules/local/create_data_directories'
include { GET_CALIBRATION_SOLUTION } from '../modules/local/get_calibration_solution'

include { GET_VCS_DATA             } from '../subworkflows/local/get_vcs_data'
include { PROCESS_PSRFITS          } from '../subworkflows/local/process_psrfits'
include { PROCESS_VDIF             } from '../subworkflows/local/process_vdif'

def is_pointing = params.psrs != null ? false : true

def round_down_time_chunk(time_sec) {
    if (!(time_sec instanceof Number)) {
        throw new IllegalArgumentException("Time must be a number")
    }
    def new_time = (time_sec / 8).toInteger() * 8

    return new_time
}

def compute_start_time(obsid, offset) {
    if (!(obsid instanceof Number)) {
        throw new IllegalArgumentException("Obs ID must be a number")
    }
    if (!(offset instanceof Number)) {
        throw new IllegalArgumentException("Offset must be a number")
    }
    return obsid + offset
}

def compute_duration(obsid, offset, duration) {
    if (!(obsid instanceof Number)) {
        throw new IllegalArgumentException("Obs ID must be a number")
    }
    if (!(offset instanceof Number)) {
        throw new IllegalArgumentException("Offset must be a number")
    }
    if (!(duration instanceof Number)) {
        throw new IllegalArgumentException("Duration must be a number")
    }
    def start_time = obsid + offset
    def end_time = round_down_time_chunk(start_time) + round_down_time_chunk(duration) - 8

    return end_time - start_time
}

workflow VCS_BF {
    if (params.download || params.download_only) {
        //
        // Download and move data
        //
        GET_VCS_DATA (
            params.obsid,
            params.offset,
            params.duration,
            params.asvo_id_obs,
            params.asvo_dir,
            params.vcs_dir
        ).set { files_ready }
    } else {
        Channel
            .of(true)
            .set { files_ready }
    }

    if (!params.download_only) {
        //
        // Create channel of sources (pulsars or pointings)
        //
        if (params.psrs != null) {
            Channel
                .from(params.psrs.split(' '))
                .set { sources }
        } else if (params.pointings != null) {
            Channel
                .from(params.pointings.split(' '))
                .set { sources }
        } else if (params.pointings_file != null) {
            Channel
                .fromPath(params.pointings_file)
                .splitCsv()
                .flatten()
                .set { sources }
        } else {
            System.err.println('ERROR: No pulsars or pointings specified')
        }

        //
        // Check that the obs ID directory and subdirectories exist
        //
        CHECK_OBS_DIRECTORY (
            files_ready,
            params.vcs_dir,
            params.obsid
        )

        //
        // Create dataproduct directories or backup existing data
        //
        CREATE_DATA_DIRECTORIES (
            params.fits,
            params.vdif,
            CHECK_OBS_DIRECTORY.out.pointings_dir.first(),
            sources,
            compute_duration(
                params.obsid,
                params.offset,
                params.duration
            )
        )

        if (params.calid != null) {
            //
            // Retrieve the metafits and calibration solution
            //
            GET_CALIBRATION_SOLUTION (
                files_ready,
                params.obsid,
                params.calid
            )

            obs_metafits = GET_CALIBRATION_SOLUTION.out.obsmeta
            cal_metafits = GET_CALIBRATION_SOLUTION.out.calmeta
            cal_solution = GET_CALIBRATION_SOLUTION.out.calsol
        } else {
            obs_metafits = Channel.empty()
            cal_metafits = Channel.empty()
            cal_solution = Channel.empty()
        }

        if (params.fits) {
            //
            // Beamform and search PSRFITS
            //
            PROCESS_PSRFITS (
                sources,
                is_pointing,
                CREATE_DATA_DIRECTORIES.out.pointings_dir.first(),
                CHECK_OBS_DIRECTORY.out.data_dir.first(),
                compute_duration (
                    params.obsid,
                    params.offset,
                    params.duration
                ),
                compute_start_time (
                    params.obsid,
                    params.offset
                ),
                params.low_chan,
                params.num_chan,
                params.flagged_tiles,
                obs_metafits.first(),
                cal_metafits.first(),
                cal_solution.first(),
                params.skip_bf,
                params.ephemeris_dir,
                params.force_psrcat,
                params.nbin,
                params.nsub,
                params.npart,
                params.nosearch_prepfold,
                params.acacia_profile,
                params.acacia_bucket,
                params.acacia_prefix
            )
        }

        if (params.vdif) {
            //
            // Beamform and search VDIF
            //
            PROCESS_VDIF (
                sources.collect(),
                is_pointing,
                CREATE_DATA_DIRECTORIES.out.pointings_dir,
                CHECK_OBS_DIRECTORY.out.data_dir,
                compute_duration (
                    params.obsid,
                    params.offset,
                    params.duration
                ),
                compute_start_time (
                    params.obsid,
                    params.offset
                ),
                params.low_chan,
                params.num_chan,
                params.flagged_tiles,
                obs_metafits,
                cal_metafits,
                cal_solution,
                params.skip_bf,
                params.ephemeris_dir,
                params.force_psrcat,
                params.nbin,
                params.fine_chan,
                params.tint,
                params.pdmp_mc,
                params.pdmp_ms,
                params.nosearch_pdmp,
                params.acacia_profile,
                params.acacia_bucket,
                params.acacia_prefix
            )
        }
    }
}
