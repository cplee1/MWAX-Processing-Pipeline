//
// Download from ASVO, beamform, fold, search, plot, and upload to Acacia
//

include { CREATE_DIR_STRUCTURE     } from '../modules/local/create_dir_structure'
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
            params.vcs_dir,
            params.num_dl_jobs
        ).set { files_ready }
    } else {
        Channel
            .value(true)
            .set { files_ready }
    }

    if (!params.download_only) {
        //
        // Create channel of sources (pulsars or pointings)
        //
        if (params.psrs != null) {
            Channel
                .value(params.psrs.split(' '))
                .set { sources }
        } else if (params.pointings != null) {
            Channel
                .value(params.pointings.split(' '))
                .set { sources }
        } else if (params.pointings_file != null) {
            Channel
                .fromPath(params.pointings_file)
                .splitCsv()
                .set { sources }
        } else {
            System.err.println('ERROR: No pulsars or pointings specified')
        }

        //
        // Check observation directory and create sub-directories
        //
        CREATE_DIR_STRUCTURE (
            files_ready,
            params.vcs_dir,
            params.obsid,
            compute_duration (
                params.obsid,
                params.offset,
                params.duration
            ),
            params.fits,
            params.vdif,
            params.skip_bf,
            sources
        )

        obs_dirs = CREATE_DIR_STRUCTURE.out
            .map { ['data':it[0], 'pointings':it[1]] }
            .first()

        if (params.calid != null) {
            //
            // Retrieve the metafits and calibration solution
            //
            GET_CALIBRATION_SOLUTION (
                files_ready,
                params.obsid,
                params.calid
            )

            cal_files = GET_CALIBRATION_SOLUTION.out
                .map { ['obs_meta':it[0], 'cal_meta':it[1], 'cal_sol':it[2]] }
                .first()
        } else {
            cal_files = Channel.empty()
        }

        if (params.fits) {
            //
            // Beamform and search PSRFITS
            //
            PROCESS_PSRFITS (
                sources,
                is_pointing,
                obs_dirs,
                cal_files,
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
                sources,
                is_pointing,
                obs_dirs,
                cal_files,
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
