import json
import asyncio
import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from dotenv import load_dotenv
import os
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

app = FastAPI()

BINANCE_WS_URL = os.getenv('BINANCE_WS_URL')

# List of symbols to track
SYMBOLS = ["solusdt", "btcusdt", "ethusdt", "bnbusdt", "adausdt", "xrpusdt"]
# 1-second Kline streams
STREAMS = "/".join([f"{symbol}@kline_1s" for symbol in SYMBOLS])
BINANCE_WS_FULL_URL = f"{BINANCE_WS_URL}{STREAMS}"

# Store active frontend connections
active_connections = set()


async def binance_websocket():
    """Handles Binance WebSocket connection and forwards data to clients."""
    global binance_task
    while active_connections:  # Only run if there are active connections
        try:
            async with websockets.connect(BINANCE_WS_FULL_URL, ping_interval=20, ping_timeout=60) as ws:
                print("Connected to Binance WebSocket")

                while active_connections:  # Keep running as long as clients are connected
                    res = await ws.recv()
                    data = json.loads(res)

                    if "data" in data and "k" in data["data"]:
                        kline = data["data"]["k"]
                        symbol = data["data"]["s"].upper()

                        formatted_data = {
                            "symbol": symbol,
                            "timestamp": data["data"]["E"],
                            "open": kline["o"],
                            "high": kline["h"],
                            "low": kline["l"],
                            "close": kline["c"],
                            "volume": kline["v"],
                            "is_closed": kline["x"],
                        }

                        print(
                            f"Formatted Data Sent to Clients: {formatted_data}")

                        for conn in active_connections.copy():
                            try:
                                await conn.send_text(json.dumps(formatted_data))
                            except:
                                active_connections.remove(conn)

                    await asyncio.sleep(1)  # Fetch updates every 1 second

        except Exception as e:
            print(f"WebSocket Error: {e}, reconnecting in 10 seconds...")
            await asyncio.sleep(10)  # Wait before reconnecting

    print("No active clients. Stopping Binance WebSocket.")


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Manages WebSocket connections with frontend."""
    await websocket.accept()

    # If this is the first connection, start Binance WebSocket
    if not active_connections:
        print("First client connected, starting Binance WebSocket...")
        asyncio.create_task(binance_websocket())

    active_connections.add(websocket)

    try:
        while True:
            await websocket.receive_text()  # Keep connection alive
    except WebSocketDisconnect:
        print("Frontend WebSocket disconnected")
        active_connections.remove(websocket)

        # If no clients are connected, stop Binance WebSocket
        if not active_connections:
            print("No active connections. Stopping WebSocket.")
    finally:
        if websocket.client_state != WebSocketDisconnect:  # Prevent double closing
            await websocket.close()


@app.get("/")
def read_root():
    return {"message": "FastAPI WebSocket Server is Running"}
