# cython: language_level=3

cdef class HttpSession:
    cdef public response_class
    cdef public request_class
    cdef public _auth
    cdef public _loop
    cdef public connector

    cpdef request(self, unicode method, unicode url, auth=*, dict kwargs=*)
