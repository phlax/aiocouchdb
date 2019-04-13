# distutils: define_macros=CYTHON_TRACE_NOGIL=1
# cython: linetrace=True
# cython: binding=True
# cython: language_level=3
# -*- coding: utf-8 -*-

from aiocouchdb.client.resource cimport Resource
from .security cimport DatabaseSecurity


cdef class Database:
    cdef public Resource resource
    cdef public DatabaseSecurity _security
    cdef public unicode _dbname

    cdef public document_class
    cdef public design_document_class
    cdef public view_class
    cdef public security_class
