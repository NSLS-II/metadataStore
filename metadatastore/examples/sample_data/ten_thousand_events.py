from __future__ import division
from metadatastore.api import (insert_event, insert_event_descriptor,
                               find_events, insert_run_stop)
import numpy as np
from metadatastore.examples.sample_data import common

delta_r = 0.01


def rect(r, theta):
    """theta in degrees

    returns tuple; (float, float); (x,y)
    """
    x = r * np.cos(np.radians(theta))
    y = r * np.sin(np.radians(theta))
    return x,y


@common.example
def run(run_start_uid=None, sleep=0, num_events=10000):
    # Create Event Descriptors
    data_keys1 = {'point_det': dict(source='PV:ES:PointDet', dtype='number'),
                  'mtr': dict(source='PV:ES:Mtr', dtype='number')}
    desc1_uid = insert_event_descriptor(run_start=run_start_uid,
                                        data_keys=data_keys1,
                                        time=common.get_time())
    rs = np.random.RandomState(5)
    events = []
    # Point Detector Events
    base_time = common.get_time()
    for r in range(num_events):
        r /= delta_r
        det, mtr = rect(r, r)
        time = float(2 * r + 0.5 * rs.randn()) + base_time
        data = {'point_det': det, 'mtr': mtr}
        timestamps = {'point_det': time, 'mtr': time}
        event_dict = dict(descriptor=desc1_uid, seq_num=r,
                          time=time, data=data, timestamps=timestamps)
        event_uid = insert_event(**event_dict)
        # grab the actual event from metadatastore
        event, = find_events(uid=event_uid)
        events.append(event)

    #todo insert run stop if run_start_uid is not None
    if run_start_uid:
        run_stop_uid = insert_run_stop(run_start=run_start_uid,
                                       time=events[-1].time)
    return events


if __name__ == '__main__':
    import metadatastore.api as mdsc
    blc_uid = mdsc.insert_beamline_config({}, time=common.get_time())
    run_start_uid = mdsc.insert_run_start(scan_id=3022013,
                                          beamline_id='testbed',
                                          beamline_config=blc_uid,
                                          owner='tester',
                                          group='awesome-devs',
                                          project='Nikea',
                                          time=common.get_time())

    print('beamline_config_uid = %s' % blc_uid)
    print('run_start_uid = %s' % run_start_uid)
    run(run_start_uid)
