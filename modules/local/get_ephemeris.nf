process GET_EPHEMERIS {
    label 'psranalysis'

    tag "${psr}"

    errorStrategy {
        log.info("task ${task.hash} failed with code ${task.exitStatus}")
        if ( task.exitStatus == 2 ) {
            log.info('Pulsar name string is blank.')
        } else if ( task.exitStatus == 3 ) {
            log.info("Pulsar ${psr} not found in catalogue.")
        }
        return 'ignore'
    }

    input:
    tuple val(psr), val(vcsbeam_files)
    val(ephemeris_dir)
    val(force_psrcat)

    output:
    tuple val(psr), path("${psr}.par"), val(vcsbeam_files)

    script:
    if ( force_psrcat ) {
        """
        if [[ -z ${psr} ]]; then
            echo "Error: Pulsar name string is blank."
            exit 2
        fi

        par_file=${psr}.par
        psrcat -v || true
        psrcat -e ${psr} > \$par_file
        if [[ ! -z \$(grep WARNING \$par_file) ]]; then
            echo "Error: Pulsar not in catalogue."
            exit 3
        fi

        if [[ "${vcsbeam_files[0]}" == *.fits ]]; then
            # TEMPO1 compatibility modifications
            # ----------------------------------
            # Convert TCB to TDB
            time_standard=\$(cat \$par_file | grep UNITS | awk '{print \$2}')
            if [[ \$time_standard == 'TCB' ]]; then
                par_file_tcb=${psr}_TCB.par
                mv \$par_file \$par_file_tcb
                tempo2 -gr transform \$par_file_tcb \$par_file back
            fi
            # Replace TAI with BIPM
            sed -i "s/TT(TAI)/TT(BIPM)/" \$par_file
            # Replace BIPMyyyy with BIPM
            sed -i 's/TT(BIPM[0-9]\\{4\\})/TT(BIPM)/g' \$par_file
        fi
        """
    } else {
        """
        if [[ -z ${psr} ]]; then
            echo "Error: Pulsar name string is blank."
            exit 2
        fi

        par_file=${psr}.par

        if [[ -r ${ephemeris_dir}/\$par_file ]]; then
            # Preference is to use MeerTime ephemeris
            cp ${ephemeris_dir}/\$par_file \$par_file
        else
            # Otherwise, use ATNF catalogue
            echo "MeerKAT ephemeris not found. Using PSRCAT."
            psrcat -v || true
            psrcat -e ${psr} > \$par_file
            if [[ ! -z \$(grep WARNING \$par_file) ]]; then
                echo "Error: Pulsar not in catalogue."
                exit 3
            fi
        fi

        if [[ "${vcsbeam_files[0]}" == *.fits ]]; then
            # TEMPO1 compatibility modifications
            # ----------------------------------
            # Convert TCB to TDB
            time_standard=\$(cat \$par_file | grep UNITS | awk '{print \$2}')
            if [[ \$time_standard == 'TCB' ]]; then
                par_file_tcb=${psr}_TCB.par
                mv \$par_file \$par_file_tcb
                tempo2 -gr transform \$par_file_tcb \$par_file back
            fi
            # Replace TAI with BIPM
            sed -i "s/TT(TAI)/TT(BIPM)/" \$par_file
            # Replace BIPMyyyy with BIPM
            sed -i 's/TT(BIPM[0-9]\\{4\\})/TT(BIPM)/g' \$par_file
        fi
        """
    }
}
