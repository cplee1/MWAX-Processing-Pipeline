include { spsr           } from '../modules/singlepixel_module'
include { spt            } from '../modules/singlepixel_module'
include { dspsr_wf       } from '../modules/singlepixel_module'
include { prepfold_wf    } from '../modules/singlepixel_module'
include { mpsr           } from '../modules/multipixel_module'
include { mpt            } from '../modules/multipixel_module'

workflow bf {
    if ( params.psrs ) {
        // Beamform and fold/search catalogued pulsars
        Channel.from(params.psrs.split(' '))
            | set { psrs }
        
        if ( params.fits ) {
            if ( params.skip_bf ) {
                prepfold_wf(psrs)  // Fold PSRFITS data
            } else {
                psrs | collect | mpsr  // Multipixel beamform on pulsars
            }
        }
        if ( params.vdif ) {
            if ( params.skip_bf ) {
                dspsr_wf(psrs)  // Fold VDIF data
            } else {
                spsr(psrs)  // Singlepixel beamform on pulsars
            }
        }
    } else if ( params.pointings ||  params.pointings_file ) {
        // Beamform on pointings
        if ( params.pointings ) {
            // Get pointings from command line input
            Channel.from(params.pointings.split(' '))
                | map { pointing -> [ pointing.split('_')[0], pointing.split('_')[1] ] }
                | set { pointings }
        } else if ( params.pointings_file ) {
            // Get pointings from file
            Channel.fromPath(params.pointings_file)
                | splitCsv
                | flatten
                | map { pointing -> [ pointing.split('_')[0], pointing.split('_')[1] ] }
                | set { pointings }
        }
        if ( params.skip_bf ) {
            log.info('Custom pointings are not folded, and thus not compatible with --skip_bf. Exiting.')
            exit(1)
        } else {
            if ( params.fits ) {
                mpt(pointings)  // Multipixel beamform on pointings
            }
            if ( params.vdif ) {
                spt(pointings)  // Singlepixel beamform on pointings
            }
        }
    } else {
        log.info('No pulsars or pointings specified. Exiting.')
        exit(1)
    }
}
