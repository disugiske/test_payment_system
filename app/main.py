from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy import select
from starlette import status
from starlette.responses import JSONResponse

from app.api.payments import router as payments_router
from app.database import async_session_factory
from app.logging_config import setup_logging
from app.schemas import LivenessStatus, DBStatus


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    yield


app = FastAPI(
    title="Payments service",
    version="1.0.0",
    summary="Asynchronous payments processing microservice",
    lifespan=lifespan,
)

app.include_router(payments_router)


@app.get("/system/health", tags=["system"])
async def health() -> dict:
    return {"status": "ok"}


@app.get("/system/probes/readiness", response_class=JSONResponse)
async def readiness_probe() -> JSONResponse:
    return JSONResponse(status_code=status.HTTP_200_OK, content={"status": "ok"})


@app.get("/system/probes/liveness", response_model=LivenessStatus)
async def liveness_probe() -> LivenessStatus:
    async with async_session_factory() as session:
        await session.execute(select(1))
    return LivenessStatus(
        status="ok",
        db=DBStatus(status="ok", error=None),
    )


@app.get("/system/probes/startup", response_class=JSONResponse)
async def startup_probe() -> JSONResponse:
    return JSONResponse(status_code=status.HTTP_200_OK, content={"status": "ok"})