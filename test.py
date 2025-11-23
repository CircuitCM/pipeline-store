import asyncio

from pipes import Pipeline, Step, pipe_funcdict


def _add_one(x: int) -> int:
    return x + 1


def _add_two(x: int) -> int:
    return x + 2


def main() -> None:
    class SimplePipeline(Pipeline):
        _functions = pipe_funcdict(_add_one, _add_two)

    pipeline = SimplePipeline(steps=[Step("_add_one"), Step("_add_two")])
    result = asyncio.run(pipeline(1))
    assert result == 4, f"Unexpected pipeline result: {result}"
    print("Import test passed")


if __name__ == "__main__":
    main()
