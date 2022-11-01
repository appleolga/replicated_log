import asyncio
import json
import logging
from collections import namedtuple
from typing import List, Dict

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
import websockets

websockets.Origin


class MessageId:
    counter = 0

    def __init__(self):
        MessageId.counter += 1
        self.id = MessageId.counter

    @staticmethod
    def get_last_id():
        return MessageId.counter


Message = namedtuple('Message', ['id', 'message'])


class Log:
    def __init__(self):
        self.message_log: Dict[int, str] = {}
        self.message_log_id_to_send: int = 1

    def send_log(self):
        prefix = f'Message #{self.message_log_id_to_send} is: '
        print(f'Current id to send is: {self.message_log_id_to_send}')
        try:
            message_log_text_to_send = self.message_log.get(self.message_log_id_to_send)
        except KeyError:
            message_log_text_to_send = "This message was not recorded!"
        if self.message_log_id_to_send == max(self.message_log.keys()):
            message_log_text_to_send += ". This is the last recorded message!"
        else:
            self.message_log_id_to_send += 1
        log_record_to_send = f'{prefix}{message_log_text_to_send}'
        return log_record_to_send

    def update_message_log(self, message: Message):
        self.message_log[int(message.id)] = message.message
        self.message_log_id_to_send = 1


class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.secondaries_hosts: List[str] = []
        self.ack_counter_dict: Dict[int, Dict[str, int]] = {}

    async def accept_connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        print(self.active_connections)

    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def send_master_message(self, master_uri: str, message: str, prefix: str):
        logging.log('entered send_master_message')
        logging.log(f'ws://{master_uri}/ws/secondary')

        async with websockets.connect(f'ws://{master_uri}/ws/secondary') as master_link:
            await master_link.send(prefix + message)
            print(f'message {prefix} was sent to master')

    def check_ack(self, key):
        print(self.ack_counter_dict[key])
        return sum(self.ack_counter_dict[key].values())

        # function to initialize master output client to forward messages to secondary
        # and get ack
    async def replicate(self, message: Message):
        print('entered replicate')
        secondaries_no = len(self.secondaries_hosts)
        data = json.dumps(message._asdict())
        # active connections include master client
        print(f'number of connected secondaries: {secondaries_no}')
        self.ack_counter_dict[message.id] = dict.fromkeys(self.secondaries_hosts, 0)
        print(f'ack_counter_dict in replicate is {self.ack_counter_dict}')
        for secondary_host in self.secondaries_hosts:
            async with websockets.connect(f'ws://{secondary_host}/ws/master') as replicate_link:
                await replicate_link.send(data)
                print(f'message was sent to {secondary_host}')
                await asyncio.sleep(1)
        print(f'checksum is: {self.check_ack(message.id) / len(self.secondaries_hosts)}')
        if self.check_ack(message.id) / len(self.secondaries_hosts) == 1:
            return f'OK! your message {message.id} was recorded'
        else:
            await asyncio.sleep(1)

