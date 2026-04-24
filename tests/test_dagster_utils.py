from __future__ import annotations

from subprocess import CompletedProcess

from dagster_project.utils import run_python_script


class _Logger:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []

    def info(self, message: str, *args) -> None:
        self.messages.append(("info", message % args if args else message))

    def warning(self, message: str, *args) -> None:
        self.messages.append(("warning", message % args if args else message))


class _Context:
    def __init__(self) -> None:
        self.log = _Logger()


def test_run_python_script_logs_output_and_returns_process(monkeypatch) -> None:
    context = _Context()
    expected = CompletedProcess(args=["python", "src/example.py"], returncode=0, stdout="ok", stderr="warn")

    def fake_run(*args, **kwargs):
        return expected

    monkeypatch.setattr("dagster_project.utils.subprocess.run", fake_run)

    result = run_python_script(context, "src/example.py", "example job")

    assert result is expected
    assert ("info", "Running example job via src/example.py") in context.log.messages
    assert ("info", "ok") in context.log.messages
    assert ("warning", "warn") in context.log.messages