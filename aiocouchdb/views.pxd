
from aiocouchdb.client.resource cimport Resource


cdef class View:
    cdef public Resource resource
