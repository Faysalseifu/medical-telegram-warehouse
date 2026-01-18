from fastapi import FastAPI

app = FastAPI(title="Medical Warehouse API")


@app.get("/health", tags=["health"])
def health() -> dict[str, str]:
    return {"status": "ok"}
