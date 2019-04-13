# distutils: define_macros=CYTHON_TRACE_NOGIL=1
# cython: linetrace=True
# cython: binding=True
# cython: language_level=3

import abc
import functools


class AuthProvider(object, metaclass=abc.ABCMeta):
    """Abstract authentication provider class."""

    @abc.abstractmethod
    def reset(self):
        """Resets provider instance to default state."""
        raise NotImplementedError

    @abc.abstractmethod
    def credentials(self):
        """Returns authentication credentials if any."""
        raise NotImplementedError

    @abc.abstractmethod
    def set_credentials(self, *args, **kwargs):
        """Sets authentication credentials."""
        raise NotImplementedError

    @abc.abstractmethod
    def apply(self, url: str, headers: dict):
        """Applies authentication routines on further request. Mostly used
        to set right `Authorization` header or cookies to pass the challenge.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def update(self, response):
        """Updates provider routines from the HTTP response data.

        :param response: :class:`aiocouchdb.client.HttpResponse` instance
        """
        raise NotImplementedError

    def wrap(self, request_func):
        """Wraps request coroutine function to apply the authentication context.
        """
        @functools.wraps(request_func)
        async def wrapper(method, url, headers, **kwargs):
            self.apply(url, headers)
            response = await request_func(
                method, url,
                headers=headers,
                **kwargs)
            self.update(response)
            return response
        return wrapper


class NoAuthProvider(AuthProvider):
    """Dummy provider to apply no authentication routines."""

    def reset(self):
        pass

    def credentials(self):
        pass

    def set_credentials(self):
        pass

    def apply(self, url, headers):
        pass

    def update(self, response):
        pass

    def wrap(self, request_func):
        return request_func
