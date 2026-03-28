import asyncio

from fastapi import APIRouter, Request
from sse_starlette.sse import EventSourceResponse

router = APIRouter()


@router.get("/api/progress")
async def progress_stream(request: Request):
    orchestrator = request.app.state.orchestrator

    async def event_generator():
        queue = orchestrator.subscribe()
        try:
            # Send current state immediately
            yield {
                "event": "progress",
                "data": orchestrator.progress.to_json(),
            }
            while True:
                if await request.is_disconnected():
                    break
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield {
                        "event": "progress",
                        "data": data,
                    }
                except TimeoutError:
                    yield {"comment": "keepalive"}
        finally:
            orchestrator.unsubscribe(queue)

    return EventSourceResponse(event_generator())
