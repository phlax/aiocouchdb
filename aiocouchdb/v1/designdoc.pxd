# distutils: define_macros=CYTHON_TRACE_NOGIL=1
# cython: linetrace=True
# cython: binding=True
# cython: language_level=3
# -*- coding: utf-8 -*-

from aiocouchdb.client.resource cimport Resource
from .document cimport Document


cdef class DesignDocument:
    cdef public Resource resource
    cdef public Document _document
    cdef public document_class
    cdef public view_class
