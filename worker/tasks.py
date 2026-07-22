import asyncio
from collections.abc import Awaitable, Callable

TaskHandler = Callable[[dict], Awaitable[dict]]

_TASK_REGISTRY: dict[str, TaskHandler] = {}


def register_task(name: str) -> Callable[[TaskHandler], TaskHandler]:
    def decorator(handler: TaskHandler) -> TaskHandler:
        if name in _TASK_REGISTRY:
            raise ValueError(f"Task '{name}' is already registered")
        _TASK_REGISTRY[name] = handler
        return handler

    return decorator


def get_task_handler(name: str) -> TaskHandler | None:
    return _TASK_REGISTRY.get(name)


@register_task("add_numbers")
async def add_numbers(payload: dict) -> dict:
    return {"sum": sum(payload["numbers"])}


@register_task("fail_me")
async def fail_me(payload: dict) -> dict:
    raise ValueError("fail_me: intentional failure requested by job payload")


@register_task("sleep_demo")
async def sleep_demo(payload: dict) -> dict:
    await asyncio.sleep(5)
    return {"message": "processed"}
