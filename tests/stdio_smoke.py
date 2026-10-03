"""Start the server over stdio, list its tools, and call nifi_about against the live NiFi.

Run from the project root: ``uv run python tests/stdio_smoke.py``.
"""

from __future__ import annotations

import json
import subprocess
import sys


def main() -> None:
    proc = subprocess.Popen(
        [sys.executable, "-m", "nifi_mcp"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        encoding="utf-8",
    )
    assert proc.stdin and proc.stdout

    def send(message: dict) -> None:
        proc.stdin.write(json.dumps(message) + "\n")
        proc.stdin.flush()

    def receive(request_id: int) -> dict:
        while True:
            line = proc.stdout.readline()
            if not line:
                raise RuntimeError("Server closed stdout")
            message = json.loads(line)
            if message.get("id") == request_id:
                return message

    try:
        send(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {"name": "smoke", "version": "0"},
                },
            }
        )
        init = receive(1)
        print("server:", init.get("result", {}).get("serverInfo"))
        send({"jsonrpc": "2.0", "method": "notifications/initialized"})

        send({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
        tools = [tool["name"] for tool in receive(2)["result"]["tools"]]
        print(f"{len(tools)} tools:", ", ".join(sorted(tools)))

        send(
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {"name": "nifi_about", "arguments": {}},
            }
        )
        result = receive(3)["result"]
        print("nifi_about:", result.get("structuredContent") or result.get("content"))
    finally:
        proc.terminate()


if __name__ == "__main__":
    main()
