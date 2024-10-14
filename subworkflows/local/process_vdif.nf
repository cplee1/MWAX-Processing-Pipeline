//
// Beamform and fold VCS pulsar observations in VDIF format
//

include { PARSE_TILE_FLAGS      } from '../../modules/local/parse_tile_flags'
include { PARSE_POINTING        } from '../../modules/local/parse_pointing'
include { GET_POINTING          } from '../../modules/local/get_pointing'
include { VCSBEAM               } from '../../modules/local/vcsbeam'
include { PUBLISH_VCSBEAM_FILES } from '../../modules/local/publish_vcsbeam_files'
include { LOCATE_VDIF_FILES     } from '../../modules/local/locate_vdif_files'
include { CREATE_TARBALL        } from '../../modules/local/create_tarball'
include { COPY_TO_ACACIA        } from '../../modules/local/copy_to_acacia'
include { GET_EPHEMERIS         } from '../../modules/local/get_ephemeris'
include { DSPSR                 } from '../../modules/local/dspsr'
include { PDMP                  } from '../../modules/local/pdmp'

workflow PROCESS_VDIF {
    take:
    sources          //      list: pulsar names or ra_decs
    is_pointing      //   boolean: whether the input is a pointing
    obs_dirs         //      dict: containing paths of data and pointings dirs
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
    fine_chan        //   integer: number of fine channels per coarse channel
    tint             //   integer: length of time integrations in seconds
    pdmp_mc          //   integer: maximum number of channels to use in search
    pdmp_ms          //   integer: maximum number of integrations to use in search
    nosearch         //   boolean: whther to skip P/DM search
    acacia_profile   //    string: Acacia profile
    acacia_bucket    //    string: Acacia bucket
    acacia_prefix    //    string: Prefix of path within bucket

    main:

    source = sources.flatten()

    if (skip_beamforming) {
        //
        // Stage in the published beamformed files
        //
        LOCATE_VDIF_FILES (
            source,
            obs_dirs.pointings,
            duration
        ).set { vcsbeam_tuple }
    } else {
        //
        // Beamform on sources
        //
        if (is_pointing) {
            PARSE_POINTING ( source.map { tuple(it.split('_')) } )
                .set { pointing_tuple }
        } else {
            GET_POINTING ( source )
                .set { pointing_tuple }
        }

        // TODO: parse convert_rts_flags
        PARSE_TILE_FLAGS (
            cal_files.cal_meta,
            flagged_tiles,
        )

        VCSBEAM (
            pointing_tuple,
            obs_dirs.data,
            duration,
            begin,
            low_chan,
            cal_files,
            PARSE_TILE_FLAGS.out
        ).set { vcsbeam_tuple }
    }

    //
    // Fold and search (P/DM) the beamformed data or upload to Acacia
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
        } else if (!skip_beamforming) {
            PUBLISH_VCSBEAM_FILES (
                vcsbeam_tuple,
                obs_dirs.pointings,
                duration
            )
        }
    } else {
        GET_EPHEMERIS (
            vcsbeam_tuple,
            ephemeris_dir,
            force_psrcat
        )
        DSPSR (
            GET_EPHEMERIS.out,
            obs_dirs.pointings,
            duration,
            num_chan,
            nbin,
            fine_chan,
            tint
        )
        if (!nosearch) {
            PDMP (
                DSPSR.out,
                obs_dirs.pointings,
                duration,
                pdmp_mc,
                pdmp_ms
            )
        }
    }
}
