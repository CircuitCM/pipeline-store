from pydantic import BaseModel, ValidationError
import asyncio
from typing import Any, Callable, Dict, List, Union, ClassVar, Awaitable
import inspect
import os

MAX_DEPTH = os.environ.get("PS_DEPTH", 1000)


async def link(*args, return_exceptions=True):
    # it's redundant to evaulate sync functions here so it assumes all callables hold awaitables.
    todo = {
        i: arg() if isinstance(arg, Callable) else arg
        for i, arg in enumerate(args)
        if isinstance(arg, (Callable, Awaitable))
    }
    if len(todo) > 0:
        fin = await asyncio.gather(*todo.values(), return_exceptions=return_exceptions)
        res = {i: res for i, res in zip(todo.keys(), fin)}
        nargs = (*(res[i] if i in res else n for i, n in enumerate(args)),)
        return nargs
    return args


class Step(BaseModel):
    function: str
    args: List[Any] = []
    kwargs: Dict[str, Any] = {}

    def __init__(self, function, *args, **data):
        if "args" in data:
            args = data.pop("args")  # might wanna join these instead of override..
        if "kwargs" in data:
            kwargs = data.pop("kwargs")
        else:
            kwargs = data

        if callable(function):
            function = function.__name__
        elif isinstance(function, str):
            function = function
        else:
            raise ValueError("Function must be a callable or a string")

        super().__init__(function=function, args=list(args), kwargs=kwargs)


class Pipeline(BaseModel):
    name: str = "NA"
    steps: List[Step] = []
    _functions: Dict[str, Callable]
    MAX_DEPTH: ClassVar[int] = MAX_DEPTH  # can be overridden as a class or even object extension.

    def __init__(self, *args, **data):
        argm = {k: v for k, v in zip(("name", "steps"), args)}
        data = argm | data
        super().__init__(**data)

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
        result = None
        glvars = kwargs
        for step in self.steps:
            func_name = step.function
            func = self._functions.get(func_name)
            p_or_k = [
                p.name
                for p in inspect.signature(func).parameters.values()
                if p.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD
            ]  # for now assume all callables don't have any funky restrictions.
            kwargs = {k: v for k, v in zip(p_or_k, args)}
            # print(kwargs)
            orr = step.kwargs | glvars
            kwargs |= {
                k: orr[k] for k in p_or_k[len(args) :] if k in orr
            }  # can be changed to override certain args instead of other way around if thats ever a need.
            # print(kwargs)
            if len(kwargs) > 0:
                # first evaluate arguments if they exist.
                argval = await link(*kwargs.values())
                # should only eval pipelines as they are serializable unless a callable given in glvars.
                kwargs = {k: res for k, res in zip(kwargs.keys(), argval)}
            if not func:
                raise ValueError(f"Function {func_name} not found")
            if asyncio.iscoroutinefunction(
                func
            ):  # to tell python a sync def is a coroutine function func._is_coroutine=aio.coroutines._is_coroutine
                result = await func(**kwargs)
            elif asyncio.iscoroutine(func):
                result = await func
            else:
                result = func(**kwargs)
            args = list(result) if type(result) is tuple else [result]

        return result
