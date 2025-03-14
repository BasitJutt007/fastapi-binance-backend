import json
import asyncio
import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from dotenv import load_dotenv
import os

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
    while True:  # Infinite loop to handle reconnects
        try:
            async with websockets.connect(BINANCE_WS_FULL_URL, ping_interval=20, ping_timeout=60) as ws:
                print("Connected to Binance WebSocket")

                while True:
                    res = await ws.recv()
                    data = json.loads(res)

                    # Debugging: Print received data from Binance
                    print(f"Received from Binance: {data}")

                    # Ensure data is valid and contains 'kline' (candlestick) info
                    if "data" in data and "k" in data["data"]:
                        kline = data["data"]["k"]
                        # Convert to uppercase to match frontend
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

                        # Debug log
                        print(
                            f"Formatted Data Sent to Clients: {formatted_data}")

                        # Send data to all connected frontend clients
                        for conn in active_connections.copy():
                            try:
                                await conn.send_text(json.dumps(formatted_data))
                            except:
                                # Remove disconnected clients
                                active_connections.remove(conn)

                    await asyncio.sleep(1)  # Binance updates every 1 second

        except Exception as e:
            print(f"WebSocket Error: {e}, reconnecting in 10 seconds...")
            await asyncio.sleep(10)  # Wait before reconnecting


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Manages WebSocket connections with frontend."""
    await websocket.accept()
    active_connections.add(websocket)

    try:
        while True:
            await websocket.receive_text()  # Keep connection alive
    except WebSocketDisconnect:
        print("Frontend WebSocket disconnected")
        active_connections.remove(websocket)
    finally:
        await websocket.close()


# Start Binance WebSocket stream when FastAPI starts
@app.on_event("startup")
async def start_binance_websocket():
    # Run WebSocket handling in background
    asyncio.create_task(binance_websocket())
