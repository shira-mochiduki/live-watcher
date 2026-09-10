from typing import Any
from fastapi import FastAPI

app = FastAPI(title="Mock Central Node")

@app.get("/health")
async def health() -> dict[str, Any]:
    return {"ok": True, "service": "mock-central-node"}

@app.post("/messages")
async def messages(message: dict[str, Any]) -> dict[str, Any]:
    print("CENTRAL NODE RECEIVED:", message)
    return {
        "accepted": True,
        "name": message.get("name"),
        "source": message.get("source"),
    }
