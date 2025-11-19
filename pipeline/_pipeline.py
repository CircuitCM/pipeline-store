from collections.abc import Collection,Sequence
from copy import deepcopy

from pydantic import BaseModel, ValidationError, PrivateAttr
import asyncio
aio=asyncio
from typing import Any, Callable, Dict, List, Union, ClassVar, Awaitable
from functools import wraps
import inspect

from pydantic.fields import ModelPrivateAttr

import pipeline._misc as utl
import os

MAX_DEPTH = os.environ.get("PS_DEPTH", 1000)


def pipe_funcdict(*funcs,savesig=True) -> ModelPrivateAttr:
    """Parse the dictionary to get a basic Pipeline instance, but not the entire dependency chain.
    
    By setting savesig=False, inspect will be called for every Step of the pipeline. This might be helpful for some reason
    however it is especially slow due to repeated inspection.
    """
    if savesig:
        return PrivateAttr({
            ( a[0] if (is_seq := isinstance(a, Sequence)) else a.__qualname__):
            ((a[1] if is_seq else a), (*(p.name for p in inspect.signature(a[1] if is_seq else a).parameters.values()
                    if p.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD),))
            for a in funcs
        })
    else:
        return PrivateAttr({
            (a[0] if (is_seq := isinstance(a, Sequence)) else a.__qualname__):
            (a[1] if is_seq else a)
            for a in funcs
        })


class Step(BaseModel):
    function: str
    args: list[Any] = []
    kwargs: dict[str, Any] = {}

    def __init__(self, function, *args, **data):
        if "args" in data:
            args = data.pop("args")  # might wanna join these instead of override..
        if "kwargs" in data:
            kwargs = data.pop("kwargs")
        else:
            kwargs = data

        if callable(function):
            function = function.__qualname__
        elif isinstance(function, str):
            function = function
        else:
            raise ValueError("Function must be a callable or a string")
        super().__init__(function=function, args=args, kwargs=kwargs)


class Pipeline(BaseModel):
    MAX_DEPTH: ClassVar[int] = MAX_DEPTH  # can be overridden as a class or even object extension.
    name: str = "NA"
    steps: list[Step] = []
    _functions: dict[str, Callable | tuple[Callable,tuple]]

        
    @classmethod
    def new(cls, name=None,steps=None, **kwargs):
        nn,sn=name is None,steps is None
        if nn and sn:return cls(**kwargs)
        argm={}
        if not nn: argm['name']=name
        if not sn:argm['steps']=steps
        return cls(**argm | kwargs)
    
    def pipe_copy(self, name=None,steps=None,deep=True):
        #Should be deep almost always, so that steps won't get shared.
        nn,sn=name is None,steps is None
        if nn and sn: return self.model_copy(deep=deep)
        argm={}
        if not nn: argm['name']=name
        if not sn:argm['steps']=steps
        return self.model_copy(update=argm,deep=deep)
        

    def __or__(self, other):
        if not isinstance(other, Step):
            raise ValueError("Only Step instances can be added to the pipeline")
        self.steps.append(other)
        return self
    
    @classmethod
    def parse_obj(cls, obj: dict) -> "Pipeline":
        # Parse the dictionary to get a basic Pipeline instance, but not the entire dependency chain.
        return cls.model_validate(obj)

    @classmethod
    def _mvr(
        cls,
        obj: Any,
        depth,
        *,
        strict: bool | None = None,
        from_attributes: bool | None = None,
        context: dict[str, Any] | None = None,
    ) -> "Pipeline":
        # Parse the dictionary to get a basic Pipeline instance
        # the args may need to be passed down the line but for now this should work.
        instance = super().model_validate(
            obj, strict=strict, from_attributes=from_attributes, context=context
        )
        for step in instance.steps:
            step.args = [
                cls._parse_nested_pipeline(value, depth + 1) if isinstance(value, dict) else value
                for value in step.args
            ]
            step.kwargs = {
                key: cls._parse_nested_pipeline(value, depth + 1) if isinstance(value, dict) else value
                for key, value in step.kwargs.items()
            }
        return instance  # not type checking here fyi

    @classmethod
    def model_validate(
        cls,
        obj: Any,
        *,
        strict: bool | None = None,
        from_attributes: bool | None = None,
        context: dict[str, Any] | None = None,
    ) -> "Pipeline":
        return cls._mvr(obj, 0, strict=strict, from_attributes=from_attributes, context=context)

    @classmethod
    def parse_raw(cls, b: Union[str, bytes], *args, **kwargs) -> "Pipeline":
        # First, parse the raw JSON to get a basic Pipeline instance
        instance = super().parse_raw(b, *args, **kwargs)
        _depth = 0
        for step in instance.steps:
            step.args = [
                cls._parse_nested_pipeline(value, _depth + 1) if isinstance(value, dict) else value
                for value in step.args
            ]
            step.kwargs = {
                key: cls._parse_nested_pipeline(value, _depth + 1) if isinstance(value, dict) else value
                for key, value in step.kwargs.items()
            }
        return instance

    @classmethod
    def _parse_nested_pipeline(cls, value: dict, _depth: int) -> Any:
        if _depth > cls.MAX_DEPTH:
            return value  # Stop recursion if maximum depth is exceeded
        try:
            return cls._mvr(value, _depth)
        except ValidationError:
            return value

    def step(self, function, *args, **kwargs):
        self | Step(function, *args, **kwargs)
        return self

    async def __call__(self, *args, **kwargs):
        """Nameless args are only evaluated in the first step, kwargs will be shared will all steps and nested pipelines if the function signature supports it."""
        # assume that the results of any function will be merged with args at the front.
        args = list(args)
        glvars = kwargs
        for step in self.steps:
            func_name = step.function
            func = self._functions.get(func_name)
            if isinstance(func,tuple):
                func,p_or_k =func[0],func[1]
            else:
                p_or_k = [ #in the future we may want to parse p_or_k once at start time instead of each time it's needed.
                    p.name
                    for p in inspect.signature(func).parameters.values()
                    if p.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD
                ]  # for now we support only simplified arguments.
            sargs=args+ step.args
            kwargs = {k: v for k, v in zip(p_or_k, sargs)}
            #args are written first for order
            orr = step.kwargs | glvars #ah
            kwargs |= {
                k: orr[k] for k in p_or_k[len(sargs) :] if k in orr
            } # the non-arg remainder is added along whatever was overwritten.
            # can be changed to override certain args instead of other way around if thats ever a need.
            # print(kwargs)
            if len(kwargs) > 0:
                # first evaluate arguments if they exist.
                argval = await utl.link(*kwargs.values())
                # should only eval pipelines as they are serializable unless a callable given in glvars.
                kwargs = {k: res for k, res in zip(kwargs.keys(), argval)}
            if not func:
                raise ValueError(f"Function {func_name} not found")
            if asyncio.iscoroutinefunction(func):  # to tell python a sync def is a coroutine function func._is_coroutine=aio.coroutines._is_coroutine
                result = await func(**kwargs)
            elif asyncio.iscoroutine(func):
                result = await func
            else:
                result = func(**kwargs)
            args = list(result) if type(result) is tuple else [result]

        return result
    
    
#Note: this cache implementation isn't suppose to save results for an entire session, or be especially quick (though performance is still good).
#It's meant to replicate the fan-in/fan-out dependency of a DAG, eg like LangGraph. This is why we fall back to objects ids as keys for non-hashables, see the demo notebook.
_CSINGLE=object()
#actually id might be unecessary
def _hash_fallback(args:Collection):
    return (arg if arg.__getattribute__('__hash__') else None if arg is None else id(arg) for arg in args)

class Cache:

    def __init__(self,c_dict:dict=None):
        self.c_dict: dict = c_dict if c_dict is not None else {}

    def cache(self):
        def wrapper1(func):
            if aio.iscoroutinefunction(func):
                #@aio.coroutine
                @wraps(func)
                def async_wrapper(*args, **kwargs):
                    key= (func.__qualname__,*_hash_fallback(args),*kwargs.keys(),*_hash_fallback(kwargs.values()))
                    result = self.c_dict.get(key,_CSINGLE)
                    if result is not _CSINGLE:
                        return result
                    else:
                        result = aio.ensure_future(func(*args, **kwargs))
                        self.c_dict[key] = result
                        #print(key)
                        return result
                #see how it saves the future, so if the completed future is awaited after completion it will instantly return
                #the result. Otherwise all awaits will accumulate on the same future until it returns.
                async_wrapper._is_coroutine=aio.coroutines._is_coroutine
                return async_wrapper
            else:
                @wraps(func)
                def sync_wrapper(*args, **kwargs):
                    key= (func.__qualname__,*_hash_fallback(args),*kwargs.keys(),*_hash_fallback(kwargs.values()))
                    result = self.c_dict.get(key,_CSINGLE)
                    if result is not _CSINGLE:
                        return result
                    else:
                        result = func(*args, **kwargs)
                        self.c_dict[key] = result
                        return result
                return sync_wrapper
            # if iscofun:
            #     async_wrapper.__name__=func.__name__
            #     async_wrapper.__
            #     return async_wrapper
            # else:
            #     sync_wrapper.__name__ = func.__name__
            #     return sync_wrapper
            #return async_wrapper if iscofun else sync_wrapper
        return wrapper1



    def clear(self):
        self.c_dict.clear()
