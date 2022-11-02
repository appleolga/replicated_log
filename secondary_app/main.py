import json
import logging
import socket
from logging.config import dictConfig

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.logger import logger

from models import ReplLog, ConnectionManager, Message
from my_log_conf import log_config

# importing utility classes for working with replicates message log,
# managing master-secondary communication, and message representations in the log

dictConfig(log_config)

MY_HOST = socket.gethostname()
MY_PORT = 3000
MY_URI = f"{MY_HOST}:{str(MY_PORT)}"
MASTER_URI = "master:8000"


r_log = ReplLog()
secondary_conn_manager = ConnectionManager()

# starting the app
app = FastAPI(debug=True)

# adding logging handlers
logger = logging.getLogger('foo-logger')

logger.info('Secondary started!')


# secondary sends its uri to master to register upon startup
@app.on_event("startup")
async def send_uri():
    await secondary_conn_manager.send_master_message(master_uri=MASTER_URI, message=MY_URI, prefix='uri')
    logger.info('secondary uri sent to master')


# exposing get method to print replicated message log from outside
# it prints one log message at a time until reaches the log's last recorded message
@app.get("/")
def print_log():
    message = r_log.send_log()
    return {f'{message[0]}:{message[1]}'}


# secondary websocket server to receive and process messages from master
@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    await secondary_conn_manager.accept_connect(websocket)
    try:
        message_text = await websocket.receive_text()
        message = json.loads(message_text)
        logger.info(f'received message in json is {message}')
        message = Message(message['id'], message['message'])
        logger.info(f'received master message is: {message}')
        r_log.update_message_log(message)
        logger.info(f'current message log is : {dict(sorted(r_log.message_log.items()))}')
        # sleep(sleep_sec)
        await secondary_conn_manager.send_master_message(master_uri=MASTER_URI,
                                                         message=f'[{int(message.id)},"{MY_URI}"]',
                                                         prefix='ACK')

    except WebSocketDisconnect:
        secondary_conn_manager.disconnect(websocket)
        logger.info(f"Client #{client_id} disconnected")


if __name__ == "__main__":
    logger.setLevel(logging.DEBUG)
    uvicorn.run(app, host=MY_HOST, port=MY_PORT)
    # uvicorn.run(secondary, host="127.0.0.2", port=3750)

logger.info("End")
