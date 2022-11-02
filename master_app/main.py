import ast
import socket
import logging
from logging.config import dictConfig

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.templating import Jinja2Templates
from fastapi.logger import logger
import uvicorn

from models import ReplLog, ConnectionManager, Message, MessageId
from my_log_conf import log_config

MY_HOST = socket.gethostname()
MY_PORT = 8000

dictConfig(log_config)
r_log = ReplLog()
master_conn_manager = ConnectionManager()

# starting master api app
app = FastAPI(name="master")

# adding logging handlers
logger = logging.getLogger('foo-logger')

logger.info('Master started!')

templates = Jinja2Templates(directory="templates")


# master input client initialization. It will live throughout all app lifecycle
@app.get("/")
async def index(request: Request):
    return templates.TemplateResponse("html_master.html", {"request": request})


# master websocket server to receive and process messages from secondary
# as well as messages from html input client
@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket):
    await master_conn_manager.accept_connect(websocket)
    logger.info(f'{websocket} connected to master server')
    try:
        while True:
            message_text = await websocket.receive_text()
            logger.info(f'received message is :{message_text}')
            # processing messages from secondary: startup registration + acknowledgements
            if websocket.url.path == '/ws/secondary':
                if message_text[:3] == 'uri':
                    master_conn_manager.secondaries_hosts.append(message_text[3:])
                    logger.info(f'current secondaries hosts list is: {master_conn_manager.secondaries_hosts}')
                elif message_text[0:3] == 'ACK':
                    ack_message = ast.literal_eval(message_text[3:])
                    logger.info(f'ack_counter_dict in main is {master_conn_manager.ack_counter_dict}')
                    master_conn_manager.ack_counter_dict[ack_message[0]][ack_message[1]] = 1
            # processing messages from master html client: log input + log output
            elif websocket.url.path == '/ws/master':
                if message_text == "getLog":
                    message = r_log.send_log()
                    await master_conn_manager.send_personal_message(message, websocket)
                else:
                    message = Message(MessageId().id, message_text)
                    logger.info(message)
                    r_log.update_message_log(message)
                    # main replication logic
                    message = await master_conn_manager.replicate(message)
                    if message:
                        logger.info(f'return from replicate is: {message}')
                    else:
                        message = 'your message was not replicated in time'
                    await master_conn_manager.send_personal_message(message, websocket)
                    logger.info(r_log.message_log)

    except WebSocketDisconnect:
        master_conn_manager.disconnect(websocket)


if __name__ == "__main__":
    uvicorn.run(app, host=MY_HOST, port=MY_PORT)

logger.info("End")
