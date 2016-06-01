from __future__ import division
import metadatastore.commands
import uuid
import time as ttime

import numpy as np
from metadatastore.examples.sample_data import common

# "Magic numbers" for this simulation
start, stop, step, points_per_step = 0, 6, 1, 7
deadband_size = 0.9
num_exposures = 17


def run(run_start=None, sleep=0, mds=metadatastore.commands):
    run_start = mds.insert_run_start(time=ttime.time(),
                                     scan_id=1,
                                     beamline_id='example',
                                     uid=str(uuid.uuid4()))
    if sleep != 0:
        raise NotImplementedError("A sleep time is not implemented for this "
                                  "example.")
    # Make the data
    ramp = common.stepped_ramp(start, stop, step, points_per_step)
    deadbanded_ramp = common.apply_deadband(ramp, deadband_size)
    rs = np.random.RandomState(5)
    point_det_data = rs.randn(num_exposures) + np.arange(num_exposures)

    # Create Event Descriptors
    data_keys1 = {'point_det': dict(source='PV:ES:PointDet', dtype='number')}
    data_keys2 = {'Tsam': dict(source='PV:ES:Tsam', dtype='number')}
    ev_desc1_uid = mds.insert_descriptor(run_start=run_start,
                                         data_keys=data_keys1,
                                         time=ttime.time(),
                                         uid=str(uuid.uuid4()))
    ev_desc2_uid = mds.insert_descriptor(run_start=run_start,
                                         data_keys=data_keys2,
                                         time=ttime.time(),
                                         uid=str(uuid.uuid4()))

    # Create Events.
    events = []

    # Point Detector Events
    base_time = ttime.time()
    for i in range(num_exposures):
        time = float(2 * i + 0.5 * rs.randn()) + base_time
        data = {'point_det': point_det_data[i]}
        timestamps = {'point_det': time}
        event_dict = dict(descriptor=ev_desc1_uid, seq_num=i,
                          time=time, data=data, timestamps=timestamps,
                          uid=str(uuid.uuid4()))
        event_uid = mds.insert_event(**event_dict)
        # grab the actual event from metadatastore
        event, = mds.find_events(uid=event_uid)
        events.append(event)
        assert event['data'] == event_dict['data']

    # Temperature Events
    for i, (time, temp) in enumerate(zip(*deadbanded_ramp)):
        time = float(time) + base_time
        data = {'Tsam': temp}
        timestamps = {'Tsam': time}
        event_dict = dict(descriptor=ev_desc2_uid, time=time,
                          data=data, timestamps=timestamps, seq_num=i,
                          uid=str(uuid.uuid4()))
        event_uid = mds.insert_event(**event_dict)
        event, = mds.find_events(uid=event_uid)
        events.append(event)
        assert event['data'] == event_dict['data']

    run_stop_uid = mds.insert_run_stop(run_start, time=time,
                                       exit_status='success',
                                       uid=str(uuid.uuid4()))

    return events


if __name__ == '__main__':
    import metadatastore.api as mdsc

    run_start = mdsc.insert_run_start(scan_id=3022013,
                                          beamline_id='testbed',
                                          owner='tester',
                                          group='awesome-devs',
                                          project='Nikea',
                                          time=ttime.time(),
                                          uid=str(uuid.uuid4()))

    run(run_start)
