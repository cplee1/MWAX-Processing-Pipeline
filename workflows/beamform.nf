// Processes
include { check_directories          } from '../modules/singlepixel_module'
include { get_calibration_solution   } from '../modules/singlepixel_module'

// Subworkflows
include { process_psrfits    } from '../subworkflows/process_psrfits'
include { process_vdif       } from '../subworkflows/process_vdif'

def is_pointing = params.psrs != null ? false : true

def round_down_time_chunk(time_sec) {
    if (!(time_sec instanceof Number)) {
        throw new IllegalArgumentException("Time must be a number")
    }
    def new_time = (time_sec / 8).toInteger() * 8

    return new_time
}

def round_up_time_chunk(time_sec) {
    if (!(time_sec instanceof Number)) {
        throw new IllegalArgumentException("Time must be a number")
    }
    def new_time = ((time_sec / 8).toInteger() + 1) * 8

    return new_time
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

    return round_down_time_chunk(start_time) + round_up_time_chunk(duration) - 8
}

workflow beamform {
    // Create channel of sources (pulsars or pointings)
    if ( params.psrs != null ) {
        Channel
            .from(params.psrs.split(' '))
            .set { sources }
    } else if ( params.pointings != null ) {
        Channel
            .from(params.pointings.split(' '))
            .set { sources }
    } else if ( params.pointings_file != null ) {
        Channel
            .fromPath(params.pointings_file)
            .splitCsv()
            .flatten()
            .set { sources }
    } else {
        System.err.println('ERROR: No pulsars or pointings specified')
    }

    // Check directories exist and backup existing data
    check_directories (
        true,
        params.fits,
        params.vdif,
        params.vcs_dir,
        params.obsid,
        sources,
        compute_duration(
            params.obsid,
            params.offset,
            params.duration
        )
    )

    // Retrieve the metafits and calibration solution
    get_calibration_solution (
        params.obsid,
        params.calid
    )

    if ( params.fits ) {
        // Beamform and search PSRFITS
        process_psrfits (
            sources,
            is_pointing,
            check_directories.out.source_dir,
            check_directories.out.pointings_dir,
            check_directories.out.data_dir,
            compute_duration(
                params.obsid,
                params.offset,
                params.duration
            ),
            params.begin,
            params.low_chan,
            params.flagged_tiles,
            get_calibration_solution.out.obsmeta,
            get_calibration_solution.out.calmeta,
            get_calibration_solution.out.calsol,
            params.skip_bf,
            params.ephemeris_dir,
            params.force_psrcat,
            params.nbin,
            params.nsub,
            params.npart,
            params.nosearch_prepfold
        )
    }

    if ( params.vdif ) {
        // Beamform and search VDIF
        process_vdif (
            sources,
            is_pointing,
            check_directories.out.source_dir,
            check_directories.out.data_dir,
            compute_duration(
                params.obsid,
                params.offset,
                params.duration
            ),
            params.begin,
            params.low_chan,
            params.flagged_tiles,
            get_calibration_solution.out.obsmeta,
            get_calibration_solution.out.calmeta,
            get_calibration_solution.out.calsol,
            params.skip_bf,
            params.ephemeris_dir,
            params.force_psrcat,
            params.nbin,
            params.fine_chan,
            params.tint,
            params.pdmp_mc,
            params.pdmp_ms,
            params.nosearch_pdmp
        )
    }
}
