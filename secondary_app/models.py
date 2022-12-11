import logging
import random
from collections import namedtuple
from typing import List, Dict
import asyncio
import websockets
from fastapi import WebSocket

ACK_DELAY = random.randint(30, 60)

logger = logging.getLogger('foo-secondary_logger')


# self-incrementing class to store ids of messages in replicated log
class MessageId:
    counter = 0

    def __init__(self):
        MessageId.counter += 1
        self.id = MessageId.counter

    @staticmethod
    def get_last_id():
        return MessageId.counter


# using named tuple to store each message to be able to access its elements by key-names
Message = namedtuple('Message', ['id', 'message'])


# utility class to manage replicated log's updates, printouts of messages etc.
class ReplLog:
    def __init__(self):
        self.message_log: Dict[int, str] = {}
        self.message_log_id_to_send: int = 1

    def send_log(self):
        prefix = f'Message #{self.message_log_id_to_send} is: '
        logger.info(f'Current id to send is: {self.message_log_id_to_send}')
        try:
            message_log_text_to_send = self.message_log[self.message_log_id_to_send]
            if self.message_log_id_to_send == max(self.message_log.keys()):
                message_log_text_to_send += ". This is the last recorded message!"
            else:
                self.message_log_id_to_send += 1
        except KeyError:
            if self.message_log_id_to_send == 1:
                prefix = '!!!'
                message_log_text_to_send = "Message log is empty"
            else:
                message_log_text_to_send = "This message was not recorded!"
        # log_record_to_send = f'{prefix}{message_log_text_to_send}'
        log_record_to_send = (prefix, message_log_text_to_send)
        return log_record_to_send

    def update_message_log(self, message: Message):
        self.message_log[int(message.id)] = message.message
        self.message_log_id_to_send = 1


# utility class that manages master-secondary communication including replication
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.secondaries_hosts: List[str] = []
        self.ack_counter_dict: Dict[int, Dict[str, int]] = {}

    async def accept_connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(self.active_connections)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    @staticmethod
    async def send_master_message(master_uri: str, message: str, prefix: str):
        logger.info('entered send_master_message')
        logger.info(f'ws://{master_uri}/ws/secondary')

        async with websockets.connect(uri=f'ws://{master_uri}/ws/secondary',
                                      ping_timeout=None,
                                      open_timeout=None) as master_link:
            await master_link.send(prefix + message)
            logger.info(f'message {prefix} was sent to master')
