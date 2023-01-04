import json
import logging
import socket
import random
import asyncio
from logging.config import dictConfig
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from models import ReplLog, ConnectionManager, Message
from my_log_conf import log_config

ACK_DELAY = random.randint(5, 15)

# PROD
MY_HOST = socket.gethostname()
MY_PORT = 3000
MY_URI = f"{MY_HOST}:{str(MY_PORT)}"
MASTER_URI = "master:8000"

#
# # TEST
# MY_HOST = '127.0.0.1'
# MY_PORT = random.randint(1200, 1500)
# MY_URI = f"{MY_HOST}:{str(MY_PORT)}"
# MASTER_URI = '127.0.0.1:8080'


dictConfig(log_config)
r_log = ReplLog()
# adding logging handlers
secondary_logger = logging.getLogger('rl_logger')
secondary_conn_manager = ConnectionManager()

# starting the app
app = FastAPI(debug=True)
secondary_logger.info(f'Secondary {MY_HOST}:{MY_PORT} started!')


# secondary sends its uri to master to register upon startup
@app.on_event("startup")
async def send_uri():
    await secondary_conn_manager.send_master_message(master_uri=MASTER_URI, message=MY_URI, prefix='uri')
    secondary_logger.info('secondary uri sent to master')
    secondary_logger.info(f'ACK_DELAY is {ACK_DELAY}')


# exposing get method to print replicated message log from outside
@app.get("/")
async def print_log():
    message = await r_log.send_log()
    return message


# secondary websocket server to receive and process messages from master
@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    await secondary_conn_manager.accept_connect(websocket)
    try:
        message_text = await websocket.receive_text()
        message = json.loads(message_text)
        secondary_logger.info(f'received message in json is {message}')
        message = Message(message['id'], message['message'])
        secondary_logger.info(f'received master message is: {message}')
        await asyncio.sleep(ACK_DELAY)
        r_log.update_message_log(message)
        secondary_logger.info(f'current message log is : {dict(r_log.message_log.items())}')
        await secondary_conn_manager.send_master_message(master_uri=MASTER_URI,
                                                         message=f'[{int(message.id)},"{MY_URI}"]',
                                                         prefix='ACK')

    except WebSocketDisconnect:
        secondary_conn_manager.disconnect(websocket)
        secondary_logger.info(f"Client #{client_id} disconnected")


if __name__ == "__main__":
    secondary_logger.setLevel(logging.DEBUG)
    uvicorn.run(app, host=MY_HOST, port=MY_PORT)

secondary_logger.info("End")
