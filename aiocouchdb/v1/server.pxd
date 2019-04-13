# distutils: define_macros=CYTHON_TRACE_NOGIL=1
# cython: linetrace=True
# cython: binding=True
# cython: language_level=3
# -*- coding: utf-8 -*-

from aiocouchdb.client.resource cimport Resource
from .authdb cimport AuthDatabase
from .config cimport ServerConfig
from .session cimport Session


cdef class Server:
    cdef public unicode authdb_name
    cdef public authdb_class
    cdef public config_class
    cdef public database_class
    cdef public session_class

    cdef public AuthDatabase _authdb
    cdef public ServerConfig _config
    cdef public Session _session
    cdef public Resource resource
