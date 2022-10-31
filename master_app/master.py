import ast
import logging
import os

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.templating import Jinja2Templates
import uvicorn

from models import Log, ConnectionManager, Message, MessageId

def create_app_instance():

    logging.basicConfig(
        format="%(message)s",
        level=logging.DEBUG,
        filename='logs/master.log',
        filemode='w'
    )

    log = Log()
    master_conn_manager = ConnectionManager()

    # starting master api app
    master = FastAPI(name="master")

    print('Master started!')
    print(f'currend wd is {os.getcwd()}')

    templates = Jinja2Templates(directory="templates")

    # master input client initialization. It will live throughout all app lifecycle
    @master.get("/")
    async def index(request: Request):
        return templates.TemplateResponse("html_master.html", {"request": request})

    @master.websocket("/ws/{client_id}")
    async def websocket_endpoint(websocket: WebSocket, client_id: str):
        await master_conn_manager.accept_connect(websocket)
        print(f'{websocket} connected to master server')
        try:
            while True:
                message_text = await websocket.receive_text()
                print(f'received message is :{message_text}')
                if websocket.url.path == '/ws/secondary':
                    if message_text[0:3]=='uri':
                        master_conn_manager.secondaries_hosts.append(message_text[3:])
                        print(f'current secondaries hosts list is: {master_conn_manager.secondaries_hosts}')
                    elif message_text[0:3]=='ACK':
                        ack_message = ast.literal_eval(message_text[3:])
                        print(f'ack_counter_dict in main is {master_conn_manager.ack_counter_dict}')
                        master_conn_manager.ack_counter_dict[ack_message[0]][ack_message[1]] = 1
                elif websocket.url.path == '/ws/master':
                    if message_text == "getLog":
                        message = log.send_log()
                        await master_conn_manager.send_personal_message(message, websocket)
                    else:
                        message = Message(MessageId().id, message_text)
                        print(message)
                        log.update_message_log(message)
                        message = await master_conn_manager.replicate(message)
                        if message:
                            print(f'return from replicate is: {message}')
                        else:
                            message = 'your message was not replicated in time'
                        await master_conn_manager.send_personal_message(message, websocket)
                        print(log.message_log)


        except WebSocketDisconnect:
            master_conn_manager.disconnect(websocket)

    #
    # if __name__ == "__main__":
    #     uvicorn.run(master, host="127.0.0.1", port=1208)

    print("End")

    return master

# app = create_app_instance()
#
# if __name__ == '__main__':
#     app.run()