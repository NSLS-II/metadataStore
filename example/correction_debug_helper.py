from metadatastore.commands import *
from metadatastore.corrections import update
if __name__ == "__main__":
    blc_uid = "0f71472e-cad2-466d-b4a2-000fff6e6baa"
    run_start_uid = "ee4a3ace-e591-4a5e-a84a-4c6940eabc5d"
    descriptor1_uid = "22815618-f419-492e-aa1b-3fddf60cafb2"
    descriptor2_uid = "9b69b1d1-213a-4a1e-a457-27075b0e95da"
    run_stop_uid = "e90041db-129d-4e51-8ec8-780d9a85f0a5"

    blc, = find_beamline_configs(uid=blc_uid)
    run_start, = find_run_starts(uid=run_start_uid)
    descriptor_1, = find_event_descriptors(uid=descriptor1_uid)
    descriptor_2, = find_event_descriptors(uid=descriptor2_uid)
    run_stop, = find_run_stops(uid=run_stop_uid)

    descriptor_1_update = update(descriptor_1)
    print(descriptor_1_update)

    run_start.foo = "bar"
    run_start_update = update(run_start)

    print(run_start_update)

    descriptor_1_find = find_event_descriptors(run_start=run_start_update)
    print(next(descriptor_1_find))
