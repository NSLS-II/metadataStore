# Write the benchmarking functions here.
# See "Writing benchmarks" in the asv docs for more information.
import uuid
import metadatastore.commands as mds
from metadatastore.utils.testing import mds_setup, mds_teardown


class TimeSuite:
    """
    An example benchmark that times the performance of various kinds
    of iterating over dictionaries in Python.
    """
    def setup(self):
        mds_setup()
        kwargs = dict(time=0., scan_id=1, uid=new_uid(), beamline_id='bench')

        # for v0.1.x
        if hasattr(mds, 'insert_beamline_config'):
            blc_id = mds.insert_beamline_config(time=0., config_params={})
            kwargs['beamline_config'] = blc_id

        self.rs_id = mds.insert_run_start(**kwargs)
        data_keys = dict(a=dict(source='', dtype='number', shape=None))
        self.desc_id = mds.insert_event_descriptor(run_start=self.rs_id,
                                                   data_keys=data_keys,
                                                   time=0., uid=new_uid())
        # Generate data to insert.
        N = 1000
        self.data = N * [dict(a=1)]
        self.timestamps = N * [dict(a=1)]
        self.i = list(range(N))

    def time_insert_1000_events(self):
        for data, timestamps, i in zip(self.data, self.timestamps, self.i):
            mds.insert_event(self.desc_id, time=0., seq_num=i, data=data,
                             timestamps=timestamps, uid=new_uid())

    def teardown(self):
        mds_teardown()


def new_uid():
    return str(uuid.uuid4())
