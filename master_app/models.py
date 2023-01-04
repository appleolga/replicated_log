import asyncio
import json
import logging
from logging.config import dictConfig
import random
import socket
from collections import namedtuple
from typing import List, Dict, Set
import websockets
from fastapi import WebSocket

from my_log_conf import log_config

# setting MAX_RETRIES == 0 provides unlimited retires
MAX_RETRIES = 0
# this parameter is used for all timeouts while opening a connection to a secondary
URI_CONNECT_TIMEOUTS = 10

# PROD
MY_HOST = socket.gethostname()
MY_PORT = 8000

# # TEST
# MY_HOST = '127.0.0.1'
# MY_PORT = 8080

dictConfig(log_config)
logger = logging.getLogger('rl_logger')


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
Message = namedtuple('Message', ['id', 'message', 'wc', 'source'])


# utility class to manage replicated log's updates, printouts of messages etc.
class ReplLog:
    def __init__(self):
        # main log data structure to store message as key = message id, value = {message text, message write concern}
        self.message_log: Dict[int, List[str, int, int]] = {}

    async def send_log(self):
        if self.message_log:
            try:
                full_mes = ''
                for k in range(1, max(self.message_log.keys()) + 1):
                    prefix = f'#{k}: '
                    message_log_text_to_send = self.message_log[k][0]
                    if k == max(self.message_log.keys()):
                        message_log_text_to_send += ". This is the last recorded message!"
                    log_record_to_send = f'{prefix}{message_log_text_to_send}'
                    full_mes += f' | {log_record_to_send} | '
            except KeyError:
                full_mes = 'There are missing messages from the log. Log cannot be retrieved'
        else:
            full_mes = 'Log is empty! '
        return full_mes

    def update_message_log(self, message: Message, source: int):
        self.message_log[int(message.id)] = [message.message, message.wc, source]
        return True


# utility class that manages nodes communication including log replication
class ConnectionManager:
    def __init__(
            self,
            secondaries_hosts: Set[str] = None,
            # data structure to keep record of received acknowledgements for a specific message
            # key = message id, value = {secondary host who sent ack, ack receipt flag (0 or 1)}
            ack_counter_dict: Dict[int, Dict[str, int]] = None,
            # data structure to keep record of retries to each connected secondary for a specific message
            # key = message id, value = {secondary host to replicate to, retries number}
            retries_counter: Dict[int, Dict[str, int]] = None,
            # this dict will keep track of all connected master clients
            master_sockets: Dict[int, WebSocket] = None
    ) -> None:
        self.master_sockets_counter = 0
        self.secondaries_hosts = secondaries_hosts if secondaries_hosts is not None else set()
        self.ack_counter_dict = ack_counter_dict if ack_counter_dict is not None else {}
        self.retries_counter = retries_counter if retries_counter is not None else {}
        self.master_sockets = master_sockets if master_sockets is not None else {}

    @staticmethod
    async def accept_connect(websocket: WebSocket):
        await websocket.accept()
        logger.info('connection accepted')

    @staticmethod
    # master server - master client communication
    async def send_websocket_message(message: str, websocket: WebSocket):
        await websocket.send_text(message)

    @staticmethod
    # master server - secondary client communication
    async def send_uri_message(uri: str, message: str, prefix: str, node_name: str):
        connect_uri = f'ws://{uri}/ws/{node_name}'
        logger.info(f'sending message {message} via {connect_uri}')
        try:
            async with websockets.connect(connect_uri,
                                          ping_timeout=URI_CONNECT_TIMEOUTS,
                                          open_timeout=URI_CONNECT_TIMEOUTS) as link:
                await link.send(prefix + message)
                logger.info('message sent')
                sent_res = 1
        except ConnectionRefusedError:
            logger.info(f'{uri} refused connection')
            sent_res = 0
        except asyncio.exceptions.TimeoutError:
            logger.info(f'{uri} timed out')
            sent_res = 0
        return sent_res

    # method to compare the number of received acknowledgements with the message's write concern and
    # generate a corresponding response to the user
    async def check_write_concern(self, message: Message):
        repl_response = None
        while True:
            await asyncio.sleep(1)
            received_acks = sum([v for k, v in self.ack_counter_dict[message.id].items() if k != 'resp'])
            if message.wc == 2 and received_acks >= 1:
                break
            elif message.wc == 3 and received_acks >= 2:
                break
            # resp flag for a message is set to -1 in case replication failed to at least one secondary
            # it is checked only for write concern == 3
            # for wc == 0, 1 or 2 it is not relevant
            elif message.wc == 3 and self.ack_counter_dict[message.id]['resp'] == -1:
                repl_response = f'FAIL! message "{message.message}" was not replicated with the required write concern!'
                break
            else:
                await asyncio.sleep(1)
        if not repl_response:
            repl_response = f'''OK! your message "{message.message}" was replicated
                                    on at least {received_acks} secondaries'''
        return repl_response

    # main replication logic
    async def replicate(self, message: Message, sec_to_replicate: set):
        logger.info('entered replicate')
        secondaries_no = len(sec_to_replicate)
        data = message._asdict()
        wc = data.pop('wc')
        data = json.dumps(data)
        logger.info(f'number of connected secondaries: {secondaries_no}, write concern is {wc}')
        # creating a copy of currently connected secondaries so that in case a new one is added
        # it does not affect current message's replication
        logger.info(f'hosts {sec_to_replicate}')
        # initializing a dictionary-counter for retries == 1
        # for all connected secondaries
        self.retries_counter[message.id] = dict.fromkeys(sec_to_replicate, 1)
        while sec_to_replicate:
            uri = sec_to_replicate.pop()
            logger.info(f'trying to replicate message to {uri}. Try number {self.retries_counter[message.id][uri]}')
            sent_ack = await self.send_uri_message(uri=uri, message=data, prefix='', node_name='master')
            if sent_ack:
                logger.info(f'message was sent to {uri}')
            else:
                self.retries_counter[message.id][uri] += 1
                if self.retries_counter[message.id][uri] == MAX_RETRIES:
                    # if MAX_RETRIES == 0 this code does not get executed (it is here for v3 functionality)
                    logger.info(f'''Stopping to try to replicate to {uri}.Exceeded retries number''')
                    logger.info(f'''Message "{message.message}" WAS NOT REPLICATED TO {uri}!!''')
                    # Closing message replication response flag
                    # Since master client should give a response to the user at some point,
                    # response flag closing happens in two cases:
                    # - all necessary acks received according to the write concern level (success)
                    # - number of retries exceeded MAX_RETRIES and
                    #       user is informed about replication failure in case wc == 3
                    self.ack_counter_dict[message.id]['resp'] = -1
                    continue
                else:
                    # not to block message replication to other secondaries each secondary is popped from
                    # the secondaries list and put back to the end of the list in case replication fails to
                    # enable later retries. it happens only in case maximum number of retries has not been yet exceeded
                    sec_to_replicate.add(uri)
                    retry_delay = 2 ** self.retries_counter[message.id][uri] + random.uniform(0, 1)
                    logger.info(f'retrying to replicate {message.message} to {uri} in {retry_delay} seconds')
                    await asyncio.sleep(retry_delay)
