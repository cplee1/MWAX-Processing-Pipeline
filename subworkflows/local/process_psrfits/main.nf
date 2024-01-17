//
// Beamform and fold VCS pulsar observations in PSRFITS format
//

include { PARSE_POINTINGS      } from '../../../modules/local/parse_pointings'
include { GET_POINTINGS        } from '../../../modules/local/get_pointings'
include { COMBINE_POINTINGS    } from '../../../modules/local/combine_pointings'
include { VCSBEAM_MULTIPIXEL   } from '../../../modules/local/vcsbeam_multipixel'
include { LOCATE_PSRFITS_FILES } from '../../../modules/local/locate_psrfits_files'
include { GET_EPHEMERIS        } from '../../../modules/local/get_ephemeris'
include { PREPFOLD             } from '../../../modules/local/prepfold'

workflow PROCESS_PSRFITS {
    take:
    source           //    string: pulsar name or ra_dec
    is_pointing      //   boolean: whether the input is a pointing
    source_dir       // directory: /path/to/<obsid>/pointings/<source>
    pointings_dir    // directory: /path/to/<obsid>/pointings
    data_dir         // directory: /path/to/<obsid>/combined
    duration         //   integer: length of data to beamform
    begin            //   integer: GPS start time of data to beamform
    low_chan         //   integer: lowest coarse channel index
    flagged_tiles    //    string: space separated list of tiles to flag
    obs_metafits     //      file: /path/to/<obsid>.metafits
    cal_metafits     //      file: /path/to/<calid>.metafits
    cal_solution     //      file: /path/to/<calsol>.bin
    skip_beamforming //   boolean: whether to skip beamforming
    ephemeris_dir    // directory: contains Jname.par files to override PSRCAT
    force_psrcat     //   boolean: whether to force using PSRCAT ephemeris
    nbin             //   integer: maximum number of phase bins
    nsub             //   integer: number of frequency subbands to use in search
    npart            //   integer: number of time integrations to use in search
    nosearch         //   boolean: whether to skip P/Pdot/DM search

    main:

    //
    // Beamform on sources
    //
    if (!skip_beamforming) {
        if (is_pointing) {
            PARSE_POINTINGS ( source.split('_') )
                .set { pointing_files }
        } else {
            GET_POINTINGS ( source )
                .set { pointing_files }
        }

        COMBINE_POINTINGS (
            pointing_files.collect(),
            cal_metafits,
            flagged_tiles
        )

        VCSBEAM_MULTIPIXEL (
            source.collect(),
            pointings_dir,
            data_dir,
            duration,
            begin,
            low_chan,
            obs_metafits,
            cal_metafits,
            cal_solution,
            COMBINE_POINTINGS.out.flagged_tiles,
            COMBINE_POINTINGS.out.pointings,
            COMBINE_POINTINGS.out.pairs
        )
    }

    //
    // Fold and search (P/Pdot/DM) the beamformed data
    //
    if (!is_pointing) {
        LOCATE_PSRFITS_FILES (
            vcsbeam.out,
            source,
            source_dir,
            duration
        )

        GET_EPHEMERIS (
            source,
            LOCATE_PSRFITS_FILES.out,
            ephemeris_dir,
            force_psrcat
        )

        PREPFOLD (
            source,
            source_dir,
            duration,
            nbin,
            nsub,
            npart,
            nosearch,
            LOCATE_PSRFITS_FILES.out,
            GET_EPHEMERIS.out
        )
    }
}
