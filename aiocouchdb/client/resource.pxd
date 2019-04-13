# cython: language_level=3

from cpython cimport bool

from .session cimport HttpSession


cdef class Resource:
    cdef public tuple METHODS
    cdef public bool debug
    cdef public _loop
    cdef public unicode url
    cdef public HttpSession session

    cpdef public _request(self, unicode method, dict kwargs=*)
    cpdef public log(self, unicode method, unicode url, auth, dict params)
    cpdef public request(
        self,
        unicode method,
        unicode path=*,
        data=*,
        dict headers=*,
        auth=*,
        bool maybe_raise=*,
        dict params=*,
        dict options=*)
