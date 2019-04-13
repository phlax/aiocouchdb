# distutils: define_macros=CYTHON_TRACE_NOGIL=1
# cython: linetrace=True
# cython: binding=True
# cython: language_level=3
# -*- coding: utf-8 -*-

from cpython cimport bool

from .client.response cimport ClientResponse


cdef class Feed:
    cdef public bool _active
    cdef public _exc
    cdef public _queue
    cdef public ClientResponse _resp
    cdef public unicode _encoding
    cpdef bool is_active(self)


cdef class JsonFeed(Feed):
    pass


cdef class ViewFeed(Feed):
    cdef public _total_rows
    cdef public _offset
    cdef public _update_seq


cdef class EventSourceFeed(Feed):
    pass


cdef class ChangesFeed(Feed):
    cdef public _last_seq


cdef class LongPollChangesFeed(ChangesFeed):
    pass
