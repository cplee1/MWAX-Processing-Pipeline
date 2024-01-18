//
// Calibrate observations using Birli and Hyperdrive
//

include { CREATE_CAL_DIRECTORIES } from '../modules/local/create_cal_directories'
include { BIRLI                  } from '../modules/local/birli'
include { GET_SOURCE_LIST        } from '../modules/local/get_source_list'
include { HYPERDRIVE             } from '../modules/local/hyperdrive'

workflow VCS_CAL {
    Channel
        .from(params.calibrators.split(' '))
        .map { tuple(it.split(':')) }
        .set { calibrators }

    calibrators
        .map { it[0] }
        .set { calids }

    CREATE_CAL_DIRECTORIES (
        calids,
        "${params.vcs_dir}/${params.obsid}/cal"
    )

    if (!params.skip_birli) {
        BIRLI (
            CREATE_CAL_DIRECTORIES.out,
            calids,
            "${params.vcs_dir}/${params.obsid}/cal",
            params.dt,
            params.df
        ).out.ready
        .collect()
        .flatten()
        .first()
        .set { uvfits_ready }
    } else {
        Channel
            .of(true)
            .set { uvfits_ready }
    }

    GET_SOURCE_LIST (
        uvfits_ready,
        calibrators,
        "${params.vcs_dir}/${params.obsid}/cal",
        params.src_catalogue,
        params.models_dir
    )

    HYPERDRIVE (
        calids,
        "${params.vcs_dir}/${params.obsid}/cal",
        params.flagged_tiles,
        params.flagged_fine_chans,
        GET_SOURCE_LIST.out
    )
}
