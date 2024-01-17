// Processes
include { parse_pointings    } from '../modules/singlepixel_module'
include { get_pointings      } from '../modules/singlepixel_module'
include { vcsbeam            } from '../modules/singlepixel_module'
include { create_tarball     } from '../modules/singlepixel_module'
include { copy_to_acacia     } from '../modules/singlepixel_module'
include { locate_vdif_files  } from '../modules/singlepixel_module'
include { get_ephemeris      } from '../modules/singlepixel_module'
include { dspsr              } from '../modules/singlepixel_module'
include { pdmp               } from '../modules/singlepixel_module'

workflow process_vdif {
    take:
    source
    is_pointing
    source_dir
    // Beamforming
    data_dir
    duration
    begin
    low_chan
    flagged_tiles
    obs_metafits
    cal_metafits
    cal_solution
    skip_beamforming
    // Folding
    ephemeris_dir
    force_psrcat
    nbin
    fine_chan
    tint
    // Searching
    pdmp_mc
    pdmp_ms
    nosearch

    main:
    if (skip_beamforming) {
        locate_vdif_files (
            source,
            source_dir,
            duration
        )
        .set { vcsbeam_files }
    } else {
        if (is_pointing) {
            parse_pointings (
                source.split('_'),
                cal_metafits,
                flagged_tiles
            )
            .set { vcsbeam_input }
        } else {
            get_pointings (
                source,
                cal_metafits,
                flagged_tiles
            )
            .set { vcsbeam_input }
        }
        vcsbeam (
            source,
            source_dir,
            data_dir,
            duration,
            begin,
            low_chan,
            obs_metafits,
            cal_metafits,
            cal_solution,
            vcsbeam_input.flagged_tiles,
            vcsbeam_input.pointings
        )
        .set { vcsbeam_files }
    }
    if (is_pointing) {
        create_tarball (
            source,
            vcsbeam_files
        )
        copy_to_acacia (
            source,
            create_tarball.out
        )
    } else {
        get_ephemeris (
            source,
            vcsbeam_files,
            ephemeris_dir,
            force_psrcat
        )
        dspsr (
            source,
            source_dir,
            duration,
            nbin,
            fine_chan,
            tint,
            vcsbeam_files,
            get_ephemeris.out
        )
        if (!nosearch) {
            pdmp (
                dspsr.out,
                source,
                source_dir,
                duration,
                pdmp_mc,
                pdmp_ms
            )
        }
    }
}
