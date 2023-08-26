import asyncio
import logging

_logger = logging.getLogger("root")


class Forwarder:
    def get_tcp_opcua_queue(self):
        return self.__queue_tcp_opcua

    def get_opcua_tcp_queue(self):
        return self.__queue_opcua_tcp

    async def read_tcp(self):
        while True:
            data = (await self.__tcp_reader.readuntil(b"\0")).decode()[:-1]
            # print("Got data from TCP:", data)
            if not data:
                _logger.error("Reading from TCP socket gave no data")
                break
            await self.__queue_tcp_opcua.put(data)

    async def write_tcp(self):
        while True:
            data = await self.__queue_opcua_tcp.get()
            print("opc_tcp:\n ", data)
            try:
                self.__tcp_writer.write(data.encode())
            except Exception as e:
                print("exception during writing", e)
            await self.__tcp_writer.drain()

    def __init__(self, tcp_reader, tcp_writer):
        self.__tcp_reader = tcp_reader
        self.__tcp_writer = tcp_writer

        self.__queue_tcp_opcua = asyncio.Queue()
        self.__queue_opcua_tcp = asyncio.Queue()
