# distutils: define_macros=CYTHON_TRACE_NOGIL=1
# cython: linetrace=True
# cython: binding=True
# cython: language_level=3
# -*- coding: utf-8 -*-

from .database cimport Database
from .document cimport Document


cdef class AuthDatabase(Database):
    pass


cdef class UserDocument(Document):
    pass
