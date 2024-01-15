// Processes
include { check_directories          } from '../modules/singlepixel_module'
include { get_calibration_solution   } from '../modules/singlepixel_module'

// Workflows
include { spsr                       } from '../modules/singlepixel_module'
include { spt                        } from '../modules/singlepixel_module'
include { dspsr_wf                   } from '../modules/singlepixel_module'
include { prepfold_wf                } from '../modules/singlepixel_module'
include { mpsr                       } from '../modules/multipixel_module'
include { mpt                        } from '../modules/multipixel_module'

workflow bf {
    if ( params.psrs ) {
        // Beamform and fold/search catalogued pulsars
        Channel.from(params.psrs.split(' '))
            | map { true, it }
            | check_directories
            | get_calibration_solution
            | set { job_info }
        
        if ( params.fits ) {
            if ( params.skip_bf ) {
                job_info
                    | map { it[0] }
                    | prepfold_wf  // Fold PSRFITS data
            } else {
                mpsr(job_info)  // Multipixel beamform on pulsars
            }
        }
        if ( params.vdif ) {
            if ( params.skip_bf ) {
                job_info
                    | map { it[0] }
                    | dspsr_wf  // Fold VDIF data
            } else {
                spsr(job_info)  // Singlepixel beamform on pulsars
            }
        }
    } else if ( params.pointings ||  params.pointings_file ) {
        // Beamform on pointings
        if ( params.pointings ) {
            // Get pointings from command line input
            Channel.from(params.pointings.split(' '))
                | map { true, it }
                | check_directories
                | get_calibration_solution
                | set { job_info }
        } else if ( params.pointings_file ) {
            // Get pointings from file
            Channel.fromPath(params.pointings_file)
                | splitCsv
                | flatten
                | map { true, it }
                | check_directories
                | get_calibration_solution
                | set { job_info }
        }
        if ( params.skip_bf ) {
            System.err.println('ERROR: Custom pointings are not folded, and thus not compatible with --skip_bf')
            exit(1)
        } else {
            if ( params.fits ) {
                mpt(job_info)  // Multipixel beamform on pointings
            }
            if ( params.vdif ) {
                spt(job_info)  // Singlepixel beamform on pointings
            }
        }
    } else {
        System.err.println('ERROR: No pulsars or pointings specified')
        exit(1)
    }
}
