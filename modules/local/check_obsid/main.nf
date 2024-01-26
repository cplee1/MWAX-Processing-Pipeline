process CHECK_OBSID {
    tag "${obsid}"

    input:
    val(obsid)

    output:
    val(true)

    script:
    """
    #!/usr/bin/env python
    import sys

    def check_obsid(string):
        if string.isdigit() and len(string) == 10:
            return True
        else:
            return False

    if not (check_obsid('${obsid}')):
        sys.exit(1)
    """
}
