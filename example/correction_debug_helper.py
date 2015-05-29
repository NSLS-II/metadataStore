from metadatastore.commands import *
from metadatastore.corrections import update
if __name__ == "__main__":
    blc_uid = "06570a85-880f-4499-a6ef-27a05c925aa6"
    run_start_uid = "712bf551-1f0a-46bb-a6ce-3c2dcb3faeb4"
    descriptor1_uid = "a9df623f-1315-496f-9166-6cfbf2b413df"
    descriptor2_uid = "0685b64c-073a-460a-8fb0-f7d33ded297b"
    run_stop_uid = "6f56aa22-aabf-40dc-9d2f-f80bca66407e"

    blc, = find_beamline_configs(uid=blc_uid)
    run_start, = find_run_starts(uid=run_start_uid)
    descriptor_1, = find_event_descriptors(uid=descriptor1_uid)
    descriptor_2, = find_event_descriptors(uid=descriptor2_uid)
    run_stop, = find_run_stops(uid=run_stop_uid)

    descriptor_1_update = update(descriptor_1)
    print(descriptor_1_update)
