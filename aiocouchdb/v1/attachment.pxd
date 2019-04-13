# distutils: define_macros=CYTHON_TRACE_NOGIL=1
# cython: linetrace=True
# cython: binding=True
# cython: language_level=3
# -*- coding: utf-8 -*-

from aiocouchdb.client.resource cimport Resource


cdef class Attachment:
    cdef public Resource resource
    cdef public unicode _name
