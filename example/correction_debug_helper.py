from metadatastore.commands import *
from metadatastore.corrections import update
if __name__ == "__main__":
    blc_uid = "76eaa3b3-f607-4a81-b155-2f6806d2d38f"
    run_start_uid = "6cf180fd-151f-4398-a8d5-e9d44317f784"
    descriptor1_uid = "b70f22df-b103-4d00-afdc-5a86e365c5a7"
    descriptor2_uid = "7eb418e2-8c63-4f97-afeb-c67b78253264"
    run_stop_uid = "2b544e8c-4eb9-4148-8cb0-b3e4191b93ad"

    blc, = find_beamline_configs(uid=blc_uid)
    run_start, = find_run_starts(uid=run_start_uid)
    print('run start from run_start_uid %s ' % run_start_uid)
    print(run_start)
    run_stop, = find_run_stops(uid=run_stop_uid)
    print("run stop from run_stop_uid %s" % run_stop_uid)
    print(run_stop)
    run_stop, = find_run_stops(run_start=run_start_uid)
    print("run stop from run_start_uid %s" % run_start_uid)
    print(run_stop)
    descriptor_1, = find_event_descriptors(uid=descriptor1_uid)
    descriptor_2, = find_event_descriptors(uid=descriptor2_uid)

    events = find_events(descriptor=descriptor1_uid)

    print(next(events))
    descriptor_1_update = update(descriptor_1)
    print(descriptor_1_update)

    run_start.foo = "bar"
    run_start_update = update(run_start)

    print(run_start_update)

    descriptor_1_find = find_event_descriptors(run_start=run_start_update)
    print(next(descriptor_1_find))
