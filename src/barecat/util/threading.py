"""Thread-local storage utilities."""


class ThreadLocalStorage:
    """Storage that lazily creates values and handles closing.

    Encapsulates the common pattern of:
    - Lazily creating a value on first access using a factory
    - Closing the value when done
    - Optionally being thread-local (each thread gets its own value)

    Usage:
        storage = ThreadLocalStorage(threadsafe=True)

        # Get or create value
        sock = storage.get(make_socket)

        # Close and clear
        storage.close()
    """

    def __init__(self, threadsafe: bool):
        self._local = get_local(threadsafe)

    def get(self, factory):
        """Get the stored value, creating it with factory() if not present."""
        try:
            return self._local.value
        except AttributeError:
            self._local.value = factory()
            return self._local.value

    def close(self):
        """Close and clear the stored value (current thread only if threadsafe)."""
        try:
            self._local.value.close()
            del self._local.value
        except AttributeError:
            pass


def get_local(threadsafe: bool):
    """Return a thread-local or shared storage object depending on threadsafe flag."""
    if threadsafe:
        import multiprocessing_utils

        return multiprocessing_utils.local()
    return SharedLocal()


class SharedLocal:
    """A dummy that acts like multiprocessing_utils.local() but isn't thread-local.

    Use this as the non-threadsafe alternative to multiprocessing_utils.local().
    Both support arbitrary attribute access, but SharedLocal shares state across
    all threads while multiprocessing_utils.local() gives each thread its own copy.
    """

    pass
