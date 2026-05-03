with open("api/main.py", "r") as f:
    text = f.read()

text = "import structlog\n" + text.replace("app = FastAPI(", "logger = structlog.get_logger(__name__)\n\napp = FastAPI(")

text = text.replace("def health() -> HealthResponse:\n    return", "def health() -> HealthResponse:\n    logger.info(\"Health check requested\")\n    return")

with open("api/main.py", "w") as f:
    f.write(text)
