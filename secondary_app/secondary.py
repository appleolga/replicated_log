import sys
from time import sleep
import json
import random
import asyncio
import logging

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.templating import Jinja2Templates
import uvicorn

from models import Log, ConnectionManager, Message

MY_HOST = "127.0.0.2"
MY_PORT = random.randint(3000, 4000)
MY_URI = f"{MY_HOST}:{str(MY_PORT)}"
MASTER_URI = "127.0.0.1:1208"

logging.basicConfig(
    format="%(message)s",
    level=logging.DEBUG,
    filename=f'logs/secondary_{MY_PORT}.log',
    filemode='w'
)

log = Log()
secondary_conn_manager = ConnectionManager()

# starting the app
secondary = FastAPI(name="secondary")

print('Secondary started!')
templates = Jinja2Templates(directory="templates")

asyncio.run(secondary_conn_manager.send_master_message(master_uri=MASTER_URI,message=MY_URI, prefix='uri'))

@secondary.get("/")
async def index(request: Request, uri: str = MY_URI):
    return templates.TemplateResponse("html_secondary.html", {"request": request, "uri" : uri})

@secondary.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    await secondary_conn_manager.accept_connect(websocket)
    print(websocket)
    # await manager.send_master_message(MY_URI, 'uri')
    try:
        while True:
            message_text = await websocket.receive_text()
            if websocket.url.path == '/ws/master':
                message = json.loads(message_text)
                print(f'received message in json is {message}')
                message = Message(message['id'], message['message'])
                print(f'received master message is: {message}')
                log.update_message_log(message)
                print(f'current message log is : {dict(sorted(log.message_log.items()))}')
                sleep(sleep_sec)
                await secondary_conn_manager.send_master_message(master_uri=MASTER_URI,
                                                                 message=f'[{int(message.id)},"{MY_URI}"]',
                                                                 prefix='ACK')
            else:
                if message_text == "getLog":
                    message = log.send_log()
                    await secondary_conn_manager.send_personal_message(message, websocket)
    except WebSocketDisconnect:
        secondary_conn_manager.disconnect(websocket)
        print (f"Client #{client_id} disconnected")




if __name__ == "__main__":
    sleep_sec = int(sys.argv[1])
    uvicorn.run(secondary, host=MY_HOST, port=MY_PORT)
    # uvicorn.run(secondary, host="127.0.0.2", port=3750)

print("End")