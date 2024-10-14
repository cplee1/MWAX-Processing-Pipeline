process CHECK_DATA_FORMAT {
    label 'python'
    shell '/usr/bin/env', 'python'

    input:
    val(obsid)

    output:
    path("${obsid}_dir.txt")

    script:
    """
    import sys
    import time
    import urllib.request
    import json
    import csv

    def get_meta(service, params, retries=3):
        data = urllib.parse.urlencode(params)

        result = None
        for _ in range(0, retries):
            err = False
            try:
                result = json.load(
                    urllib.request.urlopen(
                        "http://ws.mwatelescope.org/metadata/" + service + "?" + data
                    )
                )
            except urllib.error.HTTPError as err:
                print(f"HTTP error: code={err.code}, response: {err.read()}")
                break
            except urllib.error.URLError as err:
                print(f"URL or network error: {err.reason}")
                time.sleep(10)
                pass
            else:
                break
        return result


    def combined_deleted_check(obsid):
        # Return True if there are combined archived files available
        #
        # Note on file types:
        #   11 = raw dat
        #   14 = metafits
        #   15 = ics dat
        #   16 = combined tar
        #   17 = combined sub

        params = {"obs_id": obsid, "nocache": 1}
        files_meta = get_meta("data_files", params)

        if files_meta is None:
            print("No file metadata found")
            sys.exit(1)

        combined_archived_data = False
        for file in files_meta:

            if files_meta[file]["filetype"] in [16, 17]:
                deleted = files_meta[file]["deleted"]
                remote_archived = files_meta[file]["remote_archived"]
                if remote_archived and not deleted:
                    combined_archived_data = True
                    break
        return combined_archived_data


    def get_dir_name(obsid):
        params = {"obs_id": obsid}
        obs_meta = get_meta("obs", params)
        comb_del_check = combined_deleted_check(obsid)
        data_format = obs_meta["dataquality"]
        vcs_mode = obs_meta["mode"]

        if comb_del_check:
            data_dir = "combined"
        else:
            data_dir = "raw"

        with open(f"{obsid}_dir.txt", "w") as outfile:
            writer = csv.writer(outfile, delimiter=",")
            writer.writerow([data_dir])


    get_dir_name("${obsid}")
    """
}
