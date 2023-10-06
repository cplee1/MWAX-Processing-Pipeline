#!/usr/bin/env nextflow

include { dl } from './mwax_download'
include { cal } from './mwax_calibrate'

workflow{
    if ( ! params.obsid ) {
        println "Please specify obs ID with --obsid."
    } else if ( ! params.calibrators ) {
        println "Please specify calibrator(s) with --calibrators."
    } else if ( ! params.duration && ! params.offset ) {
        println "Please specify the duration and offset with --duration and --offset."
    } else if ( ! params.asvo_api_key ) {
        println "Please specify ASVO API key with --asvo_api_key."
    } else {
        dl | cal | view  // Download, move, and calibrate data
    }
}