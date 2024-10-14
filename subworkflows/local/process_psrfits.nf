//
// Beamform and fold VCS pulsar observations in PSRFITS format
//

include { PARSE_POINTINGS      } from '../../../modules/local/parse_pointings'
include { GET_POINTINGS        } from '../../../modules/local/get_pointings'
include { COMBINE_POINTINGS    } from '../../../modules/local/combine_pointings'
include { VCSBEAM_MULTIPIXEL as VCSBEAM } from '../../../modules/local/vcsbeam_multipixel'
include { LOCATE_PSRFITS_FILES } from '../../../modules/local/locate_psrfits_files'
include { CREATE_TARBALL       } from '../../../modules/local/create_tarball'
include { COPY_TO_ACACIA       } from '../../../modules/local/copy_to_acacia'
include { GET_EPHEMERIS        } from '../../../modules/local/get_ephemeris'
include { PREPFOLD             } from '../../../modules/local/prepfold'

workflow PROCESS_PSRFITS {
    take:
    sources          //      list: pulsar names or ra_decs
    is_pointing      //   boolean: whether the input is a pointing
    obs_dirs         //      dict: containing data dir and pointings dir
    cal_files        //      dict: containing paths of obs_meta, cal_meta, cal_sol
    duration         //   integer: length of data to beamform
    begin            //   integer: GPS start time of data to beamform
    low_chan         //   integer: lowest coarse channel index
    num_chan         //   integer: number of coarse channels
    flagged_tiles    //    string: space separated list of tiles to flag
    skip_beamforming //   boolean: whether to skip beamforming
    ephemeris_dir    // directory: contains Jname.par files to override PSRCAT
    force_psrcat     //   boolean: whether to force using PSRCAT ephemeris
    nbin             //   integer: maximum number of phase bins
    nsub             //   integer: number of frequency subbands to use in search
    npart            //   integer: number of time integrations to use in search
    nosearch         //   boolean: whether to skip P/Pdot/DM search
    acacia_profile   //    string: Acacia profile
    acacia_bucket    //    string: Acacia bucket
    acacia_prefix    //    string: Prefix of path within bucket

    main:

    source = sources.flatten()

    if (skip_beamforming) {
        //
        // Stage in the published beamformed files
        //
        LOCATE_PSRFITS_FILES (
            source,
            obs_dirs.pointings,
            duration
        ).set { vcsbeam_tuple }
    } else {
        //
        // Beamform on sources
        //
        if (is_pointing) {
            PARSE_POINTINGS ( source.map { tuple(it.split('_')) } )
                .set { pointing_files }
        } else {
            GET_POINTINGS ( source )
                .set { pointing_files }
        }

        COMBINE_POINTINGS (
            pointing_files.collate(4), // Max 4 beams
            cal_files.cal_meta,
            flagged_tiles
        )

        VCSBEAM (
            COMBINE_POINTINGS.out,
            obs_dirs.pointings,
            obs_dirs.data,
            duration,
            begin,
            low_chan,
            cal_files
        )

        VCSBEAM.out
            .splitCsv(sep: ' ')
            .map { row -> row[0] }
            .set { beamformed_sources }

        LOCATE_PSRFITS_FILES (
            beamformed_sources,
            obs_dirs.pointings,
            duration
        ).set { vcsbeam_tuple }
    }

    //
    // Fold and search (P/Pdot/DM) the beamformed data
    //
    if (is_pointing) {
        if (acacia_profile != null && acacia_bucket != null && acacia_prefix != null) {
            // Copy to <profile>/<bucket>/<prefix>/<source>.tar
            CREATE_TARBALL (
                vcsbeam_tuple
            )
            COPY_TO_ACACIA (
                CREATE_TARBALL.out,
                acacia_profile,
                acacia_bucket,
                acacia_prefix
            )
        }
    } else {
        GET_EPHEMERIS (
            vcsbeam_tuple,
            ephemeris_dir,
            force_psrcat
        )

        PREPFOLD (
            GET_EPHEMERIS.out,
            obs_dirs.pointings,
            duration,
            num_chan,
            nbin,
            nsub,
            npart,
            nosearch
        )
    }
}
