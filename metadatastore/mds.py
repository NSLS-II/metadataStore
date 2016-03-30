from __future__ import (absolute_import, division, print_function,
                        )

import six
import pymongo
from pymongo import MongoClient
import boltons.cacheutils
from . import core
from . import core_v0

_API_MAP = {0: core_v0,
            1: core}


class MDSRO(object):
    def __init__(self, config, version=1):
        self._RUNSTART_CACHE = boltons.cacheutils.LRU(max_size=1000)
        self._RUNSTOP_CACHE = boltons.cacheutils.LRU(max_size=1000)
        self._DESCRIPTOR_CACHE = boltons.cacheutils.LRU(max_size=1000)

        self.config = config

        self.__conn = None

        self.__db = None

        self.__event_col = None
        self.__descriptor_col = None
        self.__runstart_col = None
        self.__runstop_col = None
        self._api = None
        self.version = version

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, val):
        if self._api is not None:
            raise RuntimeError("Can not change api version at runtime")
        self._api = _API_MAP[val]
        self._version = val

    def disconnect(self):

        self.__conn = None

        self.__db = None

        self.__event_col = None
        self.__descriptor_col = None
        self.__runstart_col = None
        self.__runstop_col = None

    def reconfigure(self, config):
        self.disconnect()
        self.config = config

    @property
    def _connection(self):
        if self.__conn is None:
            self.__conn = MongoClient(self.config['host'],
                                      self.config.get('port', None))
        return self.__conn

    @property
    def _db(self):
        if self.__db is None:
            conn = self._connection
            self.__db = conn.get_database(self.config['database'])
        return self.__db

    @property
    def _runstart_col(self):
        if self.__runstart_col is None:
            self.__runstart_col = self._db.get_collection('run_start')

            self.__runstart_col.create_index([('uid', pymongo.DESCENDING)],
                                             unique=True)
            self.__runstart_col.create_index([('time', pymongo.DESCENDING),
                                              ('scan_id', pymongo.DESCENDING)],
                                             unique=False, background=True)

        return self.__runstart_col

    @property
    def _runstop_col(self):
        if self.__runstop_col is None:
            self.__runstop_col = self._db.get_collection('run_stop')

            self.__runstop_col.create_index([('run_start', pymongo.DESCENDING),
                                            ('uid', pymongo.DESCENDING)],
                                            unique=True)
            self.__runstop_col.create_index([('time', pymongo.DESCENDING)],
                                            unique=False, background=True)

        return self.__runstop_col

    @property
    def _descriptor_col(self):
        if self.__descriptor_col is None:
            self.__descriptor_col = self._db.get_collection('event_descriptor')

            self.__descriptor_col.create_index([('uid', pymongo.DESCENDING)],
                                               unique=True)
            self.__descriptor_col.create_index(
                [('run_start', pymongo.DESCENDING),
                 ('time', pymongo.DESCENDING)],
                unique=False, background=True)

        return self.__descriptor_col

    @property
    def _event_col(self):
        if self.__event_col is None:
            self.__event_col = self._db.get_collection('event')

            self.__event_col.create_index([('uid', pymongo.DESCENDING)],
                                          unique=True)
            self.__event_col.create_index([('descriptor', pymongo.DESCENDING),
                                           ('time', pymongo.DESCENDING)],
                                          unique=False, background=True)

        return self.__event_col

    def clear_process_cache(self):
        """Clear all local caches"""
        self._RUNSTART_CACHE.clear()
        self._RUNSTOP_CACHE.clear()
        self._DESCRIPTOR_CACHE.clear()

    def db_disconnect(self):
        """Helper function to deal with stateful connections to mongoengine"""
        self.disconnect()
        self.clear_process_cache()

    def db_connect(self, database, host, port, **kwargs):
        """Helper function to deal with stateful connections to mongoengine

        .. warning

           This will silently ignore input if the database is already
           connected, even if the input database, host, or port are
           different than currently connected.  To change the database
           connection you must call `db_disconnect` before attempting to
           re-connect.
        """
        self.clear_process_cache()
        self.reconfigure(dict(database=database,
                              host=host, port=port, **kwargs))
        return self._connection

    def run_start_given_uid(self, uid):
        """Given a uid, return the RunStart document

        Parameters
        ----------
        uid : str
            The uid

        Returns
        -------
        run_start : doc.Document
            The RunStart document.

        """
        return self._api.run_start_given_uid(uid, self._runstart_col,
                                             self._RUNSTART_CACHE)

    def run_stop_given_uid(self, uid):
        """Given a uid, return the RunStop document

        Parameters
        ----------
        uid : str
            The uid

        Returns
        -------
        run_stop : doc.Document
            The RunStop document.

        """
        return self._api.run_stop_given_uid(uid,
                                            self._runstop_col,
                                            self._RUNSTOP_CACHE,
                                            self._runstart_col,
                                            self._RUNSTART_CACHE)

    def descriptor_given_uid(self, uid):
        """Given a uid, return the EventDescriptor document

        Parameters
        ----------
        uid : str
            The uid

        Returns
        -------
        descriptor : doc.Document
            The EventDescriptor document fully de-referenced
        """
        return self._api.descriptor_given_uid(uid, self._descriptor_col,
                                              self._DESCRIPTOR_CACHE,
                                              self._runstart_col,
                                              self._RUNSTART_CACHE)

    def stop_by_start(self, run_start):
        """Given a RunStart return it's RunStop

        Raises if no RunStop exists.

        Parameters
        ----------
        run_start : doc.Document or dict or str
            The RunStart to get the RunStop for.  Can be either
            a Document/dict with a 'uid' key or a uid string

        Returns
        -------
        run_stop : doc.Document
            The RunStop document

        Raises
        ------
        NoRunStop
            If no RunStop document exists for the given RunStart
        """
        return self._api.stop_by_start(run_start,
                                       self._runstop_col,
                                       self._RUNSTOP_CACHE,
                                       self._runstart_col,
                                       self._RUNSTART_CACHE)

    def descriptors_by_start(self, run_start):
        """Given a RunStart return a list of it's descriptors

        Raises if no EventDescriptors exist.

        Parameters
        ----------
        run_start : doc.Document or dict or str
            The RunStart to get the EventDescriptors for.  Can be either
            a Document/dict with a 'uid' key or a uid string

        Returns
        -------
        event_descriptors : list
            A list of EventDescriptor documents

        Raises
        ------
        NoEventDescriptors
            If no EventDescriptor documents exist for the given RunStart
        """
        return self._api.descriptors_by_start(run_start,
                                              self._descriptor_col,
                                              self._DESCRIPTOR_CACHE,
                                              self._runstart_col,
                                              self._RUNSTART_CACHE)

    def get_events_generator(self, descriptor):
        """A generator which yields all events from the event stream

        Parameters
        ----------
        descriptor : doc.Document or dict or str
            The EventDescriptor to get the Events for.  Can be either
            a Document/dict with a 'uid' key or a uid string

        Yields
        ------
        event : doc.Document
            All events for the given EventDescriptor from oldest to
            newest
        """
        evs = self._api.get_events_generator(descriptor,
                                             self._event_col,
                                             self._descriptor_col,
                                             self._DESCRIPTOR_CACHE,
                                             self._runstart_col,
                                             self._RUNSTART_CACHE)

        # when we drop 2.7, this can be
        # yield from evs
        for ev in evs:
            yield ev

    def get_events_table(self, descriptor):
        """All event data as tables

        Parameters
        ----------
        descriptor : dict or str
            The EventDescriptor to get the Events for.  Can be either
            a Document/dict with a 'uid' key or a uid string

        Returns
        -------
        descriptor : doc.Document
            EventDescriptor document
        data_table : dict
            dict of lists of the transposed data
        seq_nums : list
            The sequence number of each event.
        times : list
            The time of each event.
        uids : list
            The uid of each event.
        timestamps_table : dict
            The timestamps of each of the measurements as dict of lists.  Same
            keys as `data_table`.
        """
        return self._api.get_events_table(descriptor,
                                          self._event_col,
                                          self._descriptor_col,
                                          self._DESCRIPTOR_CACHE,
                                          self._runstart_col,
                                          self._RUNSTART_CACHE)

    def find_run_starts(self, **kwargs):
        """Given search criteria, locate RunStart Documents.

        Parameters
        ----------
        start_time : time-like, optional
            time-like representation of the earliest time that a RunStart
            was created. Valid options are:
               - timestamps --> time.time()
               - '2015'
               - '2015-01'
               - '2015-01-30'
               - '2015-03-30 03:00:00'
               - datetime.datetime.now()
        stop_time : time-like, optional
            timestamp of the latest time that a RunStart was created. See
            docs for `start_time` for examples.
        beamline_id : str, optional
            String identifier for a specific beamline
        project : str, optional
            Project name
        owner : str, optional
            The username of the logged-in user when the scan was performed
        scan_id : int, optional
            Integer scan identifier

        Returns
        -------
        rs_objects : iterable of doc.Document objects


        Examples
        --------
        >>> find_run_starts(scan_id=123)
        >>> find_run_starts(owner='arkilic')
        >>> find_run_starts(start_time=1421176750, stop_time=time.time()})
        >>> find_run_starts(start_time=1421176750, stop_time=time.time())

        >>> find_run_starts(owner='arkilic', start_time=1421176750.514707,
        ...                stop_time=time.time())

        """
        gen = self._api.find_run_starts(self._runstart_col,
                                        self._RUNSTART_CACHE,
                                        self.config['timezone'],
                                        **kwargs)
        for rs in gen:
            yield rs

    def find_run_stops(self, **kwargs):
        """Given search criteria, locate RunStop Documents.

        Parameters
        ----------
        run_start : doc.Document or str, optional
            The RunStart document or uid to get the corresponding run end for
        start_time : time-like, optional
            time-like representation of the earliest time that a RunStop
            was created. Valid options are:
               - timestamps --> time.time()
               - '2015'
               - '2015-01'
               - '2015-01-30'
               - '2015-03-30 03:00:00'
               - datetime.datetime.now()
        stop_time : time-like, optional
            timestamp of the latest time that a RunStop was created. See
            docs for `start_time` for examples.
        exit_status : {'success', 'fail', 'abort'}, optional
            provides information regarding the run success.
        reason : str, optional
            Long-form description of why the run was terminated.
        uid : str, optional
            Globally unique id string provided to metadatastore

        Yields
        ------
        run_stop : doc.Document
            The requested RunStop documents
        """
        gen = self._api.find_run_stops(self._runstart_col,
                                       self._RUNSTART_CACHE,
                                       self._runstop_col,
                                       self._RUNSTOP_CACHE,
                                       self.config['timezone'],
                                       **kwargs)
        for rs in gen:
            yield rs

    def find_descriptors(self, **kwargs):
        """Given search criteria, locate EventDescriptor Documents.

        Parameters
        ----------
        run_start : doc.Document or str, optional
            The RunStart document or uid to get the corresponding run end for
        start_time : time-like, optional
            time-like representation of the earliest time that an
            EventDescriptor was created. Valid options are:
               - timestamps --> time.time()
               - '2015'
               - '2015-01'
               - '2015-01-30'
               - '2015-03-30 03:00:00'
               - datetime.datetime.now()
        stop_time : time-like, optional
            timestamp of the latest time that an EventDescriptor was created.
            See docs for `start_time` for examples.
        uid : str, optional
            Globally unique id string provided to metadatastore

        Yields
        -------
        descriptor : doc.Document
            The requested EventDescriptor
        """
        gen = self._api.find_descriptors(self._runstart_col,
                                         self._RUNSTART_CACHE,
                                         self._descriptor_col,
                                         self._DESCRIPTOR_CACHE,
                                         self.config['timezone'],
                                         **kwargs)
        for desc in gen:
            yield desc

    def find_events(self, **kwargs):
        """Given search criteria, locate Event Documents.

        Parameters
        -----------
        start_time : time-like, optional
            time-like representation of the earliest time that an Event
            was created. Valid options are:
               - timestamps --> time.time()
               - '2015'
               - '2015-01'
               - '2015-01-30'
               - '2015-03-30 03:00:00'
               - datetime.datetime.now()
        stop_time : time-like, optional
            timestamp of the latest time that an Event was created. See
            docs for `start_time` for examples.
        descriptor : doc.Document or str, optional
           Find events for a given EventDescriptor
        uid : str, optional
            Globally unique id string provided to metadatastore

        Returns
        -------
        events : iterable of doc.Document objects
        """
        gen = self._api.find_events(self._runstart_col,
                                    self._RUNSTART_CACHE,
                                    self._descriptor_col,
                                    self._DESCRIPTOR_CACHE,
                                    self._event_col,
                                    self.config['timezone'],
                                    **kwargs)
        for ev in gen:
            yield ev

    def find_last(self, num=1):
        """Locate the last `num` RunStart Documents

        Parameters
        ----------
        num : integer, optional
            number of RunStart documents to return, default 1

        Yields
        ------
        run_start doc.Document
           The requested RunStart documents
        """

        for ev in self._api.find_last(self._runstart_col,
                                      self._RUNSTART_CACHE,
                                      num=num):
            yield ev


class MDS(MDSRO):
    def insert_run_start(self, time, scan_id, beamline_id, uid,
                         owner='', group='', project='', **kwargs):
        '''Insert a Start document

        All extra keyword arguments are passed through to the database
        as fields in the Start document.

        Parameters
        ----------
        time : float
            The date/time as found at the client side when the run is started
        scan_id : int
            Scan identifier visible to the user and data analysis.  This is not
            a unique identifier.
        beamline_id : str
            Beamline String identifier.
        uid : str
            Globally unique id to identify this RunStart
        owner : str, optional
            A username associated with the RunStart
        group : str, optional
            An experimental group associated with the RunStart
        project : str, optional
            Any project name to help users locate the data

        Returns
        -------
        run_start : str
            uid of the inserted document.  Use `run_start_given_uid` to get
            the full document.
        '''
        if self.version == 0:
            raise NotImplementedError("Can not create documents of v0 schema")
        return core.insert_run_start(self._runstart_col,
                                     self._RUNSTART_CACHE,
                                     time, scan_id=scan_id,
                                     beamline_id=beamline_id,
                                     uid=uid,
                                     owner=owner,
                                     group=group,
                                     project=project,
                                     **kwargs)

    def insert_run_stop(self, run_start, time, uid, exit_status='success',
                        reason='', **kwargs):
        """Insert RunStop document into database

        Parameters
        ----------
        run_start : doc.Document or dict or str
            The RunStart to insert the RunStop for.  Can be either
            a Document/dict with a 'uid' key or a uid string
        time : float
            The date/time as found at the client side
        uid : str
            Globally unique id string provided to metadatastore
        exit_status : {'success', 'abort', 'fail'}, optional
            indicating reason run stopped, 'success' by default
        reason : str, optional
            more detailed exit status (stack trace, user remark, etc.)

        Returns
        -------
        run_stop : str
            uid of inserted Document

        Raises
        ------
        RuntimeError
            Only one RunStop per RunStart, raises if you try to insert a second
        """
        if self.version == 0:
            raise NotImplementedError("Can not create documents of v0 schema")
        return core.insert_run_stop(self._runstart_col,
                                    self._RUNSTART_CACHE,
                                    self._runstop_col,
                                    self._RUNSTOP_CACHE,
                                    run_start=run_start,
                                    time=time, uid=uid,
                                    exit_status=exit_status,
                                    reason=reason, **kwargs)

    def insert_descriptor(self, run_start, data_keys, time, uid, **kwargs):
        """Insert an EventDescriptor document in to database.

        Parameters
        ----------
        run_start : doc.Document or dict or str
            The RunStart to insert a Descriptor for.  Can be either
            a Document/dict with a 'uid' key or a uid string
        data_keys : dict
            Provides information about keys of the data dictionary in
            an event will contain.  No key name may include '.'.  See
            `DataKey` odm template for schema.
        time : float
            The date/time as found at the client side when an event
            descriptor is created.
        uid : str
            Globally unique id string provided to metadatastore

        Returns
        -------
        descriptor : str
            uid of inserted Document
        """
        if self.version == 0:
            raise NotImplementedError("Can not create documents of v0 schema")
        return core.insert_descriptor(self._runstart_col,
                                      self._RUNSTART_CACHE,
                                      self._descriptor_col,
                                      self._DESCRIPTOR_CACHE,
                                      run_start=run_start,
                                      data_keys=data_keys,
                                      time=time, uid=uid,
                                      **kwargs)

    def insert_event(self, descriptor, time, seq_num, data, timestamps, uid,
                     validate=False):
        """Create an event in metadatastore database backend

        .. warning

           This does not validate that the keys in `data` and `timestamps`
           match the data keys in `descriptor`.

        Parameters
        ----------
        descriptor : doc.Document or dict or str
            The Descriptor to insert event for.  Can be either
            a Document/dict with a 'uid' key or a uid string
        time : float
            The date/time as found at the client side when an event is
            created.
        seq_num : int
            Unique sequence number for the event. Provides order of an event in
            the group of events
        data : dict
            Dictionary of measured values (or external references)
        timestamps : dict
            Dictionary of measured timestamps for each values, having the
            same keys as `data` above
        uid : str
            Globally unique id string provided to metadatastore
        """
        if self.version == 0:
            raise NotImplementedError("Can not create documents of v0 schema")
        return core.insert_event(self._event_col,
                                 descriptor=descriptor,
                                 time=time, seq_num=seq_num,
                                 data=data,
                                 timestamps=timestamps,
                                 uid=uid,
                                 validate=validate)

    def bulk_insert_events(self, descriptor, events, validate):
        if self.version == 0:
            raise NotImplementedError("Can not create documents of v0 schema")
        return core.bulk_insert_events(self._event_col,
                                       descriptor=descriptor,
                                       events=events,
                                       validate=validate)
