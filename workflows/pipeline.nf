// Processes
include { asvo_vcs_download          } from '../modules/download_module'
include { check_asvo_job_files       } from '../modules/download_module'
include { check_obsid                } from '../modules/download_module'
include { move_data                  } from '../modules/download_module'
include { check_directories          } from '../modules/singlepixel_module'
include { get_calibration_solution   } from '../modules/singlepixel_module'

// Workflows
include { spsr                       } from '../modules/singlepixel_module'
include { dspsr_wf                   } from '../modules/singlepixel_module'
include { prepfold_wf                } from '../modules/singlepixel_module'
include { mpsr                       } from '../modules/multipixel_module'

workflow pipe {
    asvo_vcs_download(params.obsid)
        | check_asvo_job_files
        | check_obsid
        | move_data
    
    if ( params.psrs ) {
        // Beamform and fold/search catalogued pulsars
        Channel.from(params.psrs.split(' '))
            | set { pulsars }
        
        check_directories(move_data.out, pulsars)
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
                | set { sources }
                
            check_directories(move_data.out, sources)
                | get_calibration_solution
                | set { job_info }
        } else if ( params.pointings_file ) {
            // Get pointings from file
            Channel.fromPath(params.pointings_file)
                | splitCsv
                | flatten
                | set { sources }
            
            check_directories(move_data.out, sources)
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
