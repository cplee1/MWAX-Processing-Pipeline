//
// Beamform and fold VCS pulsar observations in VDIF format
//

include { PARSE_POINTING    } from '../../../modules/local/parse_pointing'
include { GET_POINTING      } from '../../../modules/local/get_pointing'
include { VCSBEAM           } from '../../../modules/local/vcsbeam'
include { LOCATE_VDIF_FILES } from '../../../modules/local/locate_vdif_files'
include { CREATE_TARBALL    } from '../../../modules/local/create_tarball'
include { COPY_TO_ACACIA    } from '../../../modules/local/copy_to_acacia'
include { GET_EPHEMERIS     } from '../../../modules/local/get_ephemeris'
include { DSPSR             } from '../../../modules/local/dspsr'
include { PDMP              } from '../../../modules/local/pdmp'

workflow PROCESS_VDIF {
    take:
    source           //    string: pulsar name or ra_dec
    is_pointing      //   boolean: whether the input is a pointing
    source_dir       // directory: /path/to/<obsid>/pointings/<source>
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
    fine_chan        //   integer: number of fine channels per coarse channel
    tint             //   integer: length of time integrations in seconds
    pdmp_mc          //   integer: maximum number of channels to use in search
    pdmp_ms          //   integer: maximum number of integrations to use in search
    nosearch         //   boolean: whther to skip P/DM search
    acacia_profile   //    string: Acacia profile
    acacia_bucket    //    string: Acacia bucket
    acacia_prefix    //    string: Prefix of path within bucket

    main:

    //
    // Beamform on sources
    //
    if (skip_beamforming) {
        LOCATE_VDIF_FILES (
            source,
            source_dir,
            duration
        )
        .set { vcsbeam_files }
    } else {
        if (is_pointing) {
            PARSE_POINTING (
                source.split('_'),
                cal_metafits,
                flagged_tiles
            )
            .set { vcsbeam_input }
        } else {
            GET_POINTING (
                source,
                cal_metafits,
                flagged_tiles
            )
            .set { vcsbeam_input }
        }
        VCSBEAM (
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
    }

    //
    // Fold and search (P/DM) the beamformed data or upload to Acacia
    //
    if (is_pointing) {
        if (acacia_profile != null && acacia_bucket != null && acacia_prefix != null) {
            // Copy to <profile>/<bucket>/<prefix>/<source>.tar
            CREATE_TARBALL (
                source,
                VCSBEAM.out
            )
            COPY_TO_ACACIA (
                source,
                acacia_profile,
                acacia_bucket,
                acacia_prefix,
                CREATE_TARBALL.out
            )
        }
    } else {
        GET_EPHEMERIS (
            source,
            VCSBEAM.out,
            ephemeris_dir,
            force_psrcat
        )
        DSPSR (
            source,
            source_dir,
            duration,
            nbin,
            fine_chan,
            tint,
            VCSBEAM.out,
            GET_EPHEMERIS.out
        )
        if (!nosearch) {
            PDMP (
                DSPSR.out,
                source,
                source_dir,
                duration,
                pdmp_mc,
                pdmp_ms
            )
        }
    }
}
