import asyncio as aio
from collections.abc import Awaitable, Callable


#async-util's link but I don't want to make it a dependency for the pipes's lib.
async def link(*args, return_exceptions=True,sync_wait=True):
    """Resolve callables/awaitables within a value list and return the realized values.

    Each positional value is treated as follows:

    - If it is a callable, it is called and the return value is used.
    - If it is an awaitable, it is awaited.
    - If a callable returns an awaitable, the awaitable is optionally awaited depending
      on ``sync_wait``.

    The function returns a tuple of values aligned with the input order. Awaitables are
    awaited concurrently via ``asyncio.gather``.

    :param args: Values to resolve. Each may be a literal, a callable, or an awaitable.
    :param return_exceptions: Passed through to ``asyncio.gather``.
    :param sync_wait:
        If true, a callable that returns an awaitable is always awaited. If false, only
        callables that are coroutine functions have their returned awaitable awaited.
    :returns: A tuple of resolved values in the same order as ``args``.
    """
    todo = {}
    def _prep(i, ar):
        # If it's a callable, evaluate
        if isinstance(ar, Callable):
            val = ar()
            # sync_wait: collect any Awaitable result
            # not sync_wait: only collect if the callable is a coroutine function
            if isinstance(val, Awaitable):
                #we expect iscoroutinefunction to always return a callable.
                if sync_wait or aio.iscoroutinefunction(ar):todo[i] = val
            return val
        # If the original arg is already an Awaitable, always collect it
        if isinstance(ar, Awaitable): todo[i] = ar
        return ar
    
    args = (*(_prep(i, ar) for i, ar in enumerate(args)),)

    if len(todo) > 0:
        fin = await aio.gather(*todo.values(), return_exceptions=return_exceptions)
        res = {i: res for i, res in zip(todo.keys(), fin)}
        nargs = (*(res.get(i,n) for i, n in enumerate(args)),)
        return nargs
    return args
