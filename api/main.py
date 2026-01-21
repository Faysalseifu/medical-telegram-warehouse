from fastapi import FastAPI

from .routers import channels, reports, search
from .schemas import HealthResponse

app = FastAPI(
    title="Medical Telegram Analytical API",
    description="Business-friendly analytics for Ethiopian medical Telegram channels.",
    version="0.1.0",
)

app.include_router(channels.router)
app.include_router(reports.router)
app.include_router(search.router)


@app.get("/health", tags=["health"], response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(status="ok")
