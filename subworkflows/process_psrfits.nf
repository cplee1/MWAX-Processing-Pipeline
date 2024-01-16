// Processes
include { parse_pointings        } from '../modules/multipixel_module'
include { get_pointings          } from '../modules/multipixel_module'
include { vcsbeam                } from '../modules/multipixel_module'
include { locate_psrfits_files   } from '../modules/singlepixel_module'
include { get_ephemeris          } from '../modules/singlepixel_module'
include { prepfold               } from '../modules/singlepixel_module'

workflow process_psrfits {
    take:
    source
    is_pointing
    source_dir
    pointings_dir
    // Beamforming
    data_dir
    duration
    begin
    low_chan
    obs_metafits
    cal_metafits
    cal_solution
    skip_beamforming
    // Folding
    ephemeris_dir
    force_psrcat
    nbin
    nsub
    npart
    // Searching
    nosearch

    main:
    if ( ! skip_beamforming ) {
        if ( is_pointing ) {
            parse_pointings (
                source.split('_')
            )
            .out
            .set { pointing_files }
        } else {
            get_pointings (
                source
            )
            .out
            .set { pointing_files }
        }
        combine_pointings(
            pointing_files.collect(),
            cal_metafits,
            flagged_tiles
        )
        vcsbeam (
            source.collect(),
            pointings_dir,
            data_dir,
            duration,
            begin,
            low_chan,
            obs_metafits,
            cal_metafits,
            cal_solution,
            combine_pointings.out.flagged_tiles,
            combine_pointings.out.pointings,
            combine_pointings.out.pairs
        )
    }
    if ( ! is_pointing ) {
        locate_psrfits_files (
            source,
            source_dir,
            duration
        )
        get_ephemeris (
            source,
            locate_psrfits_files.out,
            ephemeris_dir,
            force_psrcat
        )
        prepfold (
            source,
            source_dir,
            duration,
            nbin,
            nsub,
            npart,
            nosearch,
            locate_psrfits_files.out,
            get_ephemeris.out
        )
    }
}
