import ast
import socket
import asyncio
import logging
from logging.config import dictConfig
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.templating import Jinja2Templates

from models import ReplLog, ConnectionManager, Message, MessageId
from my_log_conf import log_config

# PROD
MY_HOST = socket.gethostname()
MY_PORT = 8000

# # TEST
# MY_HOST = '127.0.0.1'
# MY_PORT = 8080

dictConfig(log_config)
r_log = ReplLog()
master_conn_manager = ConnectionManager()

# starting master api app
app = FastAPI(name="master")

# adding logging handlers
master_logger = logging.getLogger('rl_logger')

master_logger.info('Master started!')
master_logger.info(f'!!!!MASTER INPUT CHAT IS AVAILABLE AT http://127.0.0.1:8080/chat')

templates = Jinja2Templates(directory="templates")


# exposing get method to print replicated message log from outside
# it prints one log message at a time until reaches the log's last recorded message
@app.get("/")
async def print_log():
    message = await r_log.send_log()
    return {f'{message[0]}:{message[1]}'}


# master input client initialization. It will live throughout app lifecycle
@app.get("/chat")
async def index(request: Request):
    return templates.TemplateResponse("html_master.html", {"request": request})


# master websocket server to receive and process messages html input client
@app.websocket("/ws/master")
async def websocket_endpoint(websocket: WebSocket):
    await master_conn_manager.accept_connect(websocket)
    master_logger.info(f'{websocket} connected to master server')
    master_conn_manager.master_socket = websocket
    repl_response = None
    try:
        while True:
            message_text = await websocket.receive_text()
            master_logger.info(f'received message is :{message_text}')
            message = Message(MessageId().id, message_text.split(',')[0], int(message_text.split(',')[1]))
            master_logger.info(message)
            master_ack = None
            master_ack = r_log.update_message_log(message)
            # main replication logic
            # adding dictionary-counter for received acknowledgements, initializing it with zeros
            master_conn_manager.ack_counter_dict[message.id] = dict.fromkeys(master_conn_manager.secondaries_hosts, 0)
            # this dictionary will also contain a flag to record if a response message to a user has been sent
            master_conn_manager.ack_counter_dict[message.id]['resp'] = 0
            # clearing html input client response area
            await master_conn_manager.send_websocket_message('', master_conn_manager.master_socket)
            if message.wc == 0:
                repl_response = f'OK! your message "{message.message}" was received!'
            elif message.wc == 1:
                if master_ack:
                    repl_response = f'OK! your message "{message.message}" was recorded on master'
            # sending response to user for write concern cases less than 3 and setting response flag == 1
            if repl_response:
                master_logger.info(repl_response)
                if master_conn_manager.ack_counter_dict[message.id]['resp'] != 1:
                    await master_conn_manager.send_websocket_message(repl_response,
                                                                     master_conn_manager.master_socket)
                    repl_response = None
                    master_conn_manager.ack_counter_dict[message.id]['resp'] = 1
            # main replication logic
            await master_conn_manager.replicate(message=message)

    except WebSocketDisconnect:
        master_logger.info('master client disconnected. Please connect it again')


# master websocket server to receive and process messages from secondaries
@app.websocket("/ws/secondary")
async def websocket_endpoint(websocket: WebSocket):
    try:
        await master_conn_manager.accept_connect(websocket)
        master_logger.info(f'{websocket} connected to master server')
        message_text = await asyncio.wait_for(websocket.receive_text(), timeout=120)
        master_logger.info(f'received message is :{message_text}')
        # processing messages from secondary: startup registration + acknowledgements
        if message_text[:3] == 'uri':
            master_conn_manager.secondaries_hosts.add(message_text[3:])
            master_logger.info(f'current secondaries hosts list is: {master_conn_manager.secondaries_hosts}')
        elif message_text[0:3] == 'ACK':
            ack_message = ast.literal_eval(message_text[3:])
            # closing acknowledgement flag for current message from current secondary
            master_conn_manager.ack_counter_dict[ack_message[0]][ack_message[1]] = 1
            master_logger.info(f'ack_counter_dict in main is {master_conn_manager.ack_counter_dict[ack_message[0]]}')
            # parsing ack message to re-create full message info (id, text and write concern)
            message = Message(ack_message[0],
                              r_log.message_log.get(ack_message[0])[0],
                              r_log.message_log.get(ack_message[0])[1])
            # checking if no response for this message has been sent yet back to user
            # and if not - proceeding to checking the number of received acks and comparing it to
            # write concern requirements to decide which message to send back to user
            # (replication success or failure)
            if master_conn_manager.ack_counter_dict[message.id]['resp'] != 1:
                repl_response = await master_conn_manager.check_write_concern(message,
                                                                              len(
                                                                                  master_conn_manager.secondaries_hosts)
                                                                              )
                await master_conn_manager.send_websocket_message(repl_response, master_conn_manager.master_socket)
                # closing response flag for current message
                master_conn_manager.ack_counter_dict[message.id]['resp'] = 1
    except WebSocketDisconnect:
        master_logger.info(dir(websocket.scope))
        master_logger.info(f'secondary {websocket.scope["uri"]} disconnected')


if __name__ == "__main__":
    uvicorn.run(app, host=MY_HOST, port=MY_PORT)

master_logger.info("End")
