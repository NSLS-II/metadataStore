from metadatastore.commands import *
from metadatastore.corrections import update
import time as ttime
from metadatastore.examples.sample_data.temperature_ramp import run

if __name__ == "__main__":
    blc_uid = insert_beamline_config({}, time=ttime.time())
    run_start1_uid = insert_run_start(scan_id=3022013,
                                      beamline_id='testbed1',
                                      beamline_config=blc_uid,
                                      owner='tester',
                                      group='awesome-devs',
                                      project='Nikea',
                                      time=ttime.time())

    run_start2_uid = insert_run_start(scan_id=100256,
                                      beamline_id='testbed2',
                                      beamline_config=blc_uid,
                                      owner='brilliant-tester',
                                      group='awesomer-devs',
                                      project='Nikea',
                                      time=ttime.time())
    ev1 = run(run_start_uid=run_start1_uid)
    ev2 = run(run_start_uid=run_start2_uid)
    descriptors = find_event_descriptors(run_start=run_start1_uid, newest=False)
    descriptor1_uid = next(descriptors).uid
    descriptor2_uid = next(descriptors).uid

    descriptors = find_event_descriptors(run_start=run_start2_uid, newest=False)
    descriptor3_uid = next(descriptors).uid
    descriptor4_uid = next(descriptors).uid

    rs, = find_run_stops(run_start=run_start1_uid)
    run_stop1_uid = rs.uid

    events = find_events(descriptor=descriptor1_uid)
    ev0 = next(events)
    print(ev0)

    # update the event descriptor and search again
    descriptor, = find_event_descriptors(uid=descriptor1_uid)
    descriptor.bad = True
    update(descriptor)

    print(descriptor)

    events = find_events(descriptor=descriptor1_uid)
    ev0 = next(events)
    print("The next event should have a Corrected EventDescriptor")
    print(ev0)

    events = find_events(descriptor=descriptor1_uid, newest=False)
    ev0 = next(events)
    print("The next event should have the original EventDescriptor")
    print(ev0)
