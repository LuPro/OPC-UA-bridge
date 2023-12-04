import logging
import asyncio

_logger = logging.getLogger("root")


class TcpSocket:
    async def connect(self, address="localhost", port=3000, name=None):
        self.__value = 0
        self.__reader, self.__writer = await asyncio.open_connection(address, port)
        if (name == None):
            name = "OPC Backend"
        # TODO: the protocol selection is still very much a prototype
        self.__writer.write(
            ('{"protocol": "native", "version": 0, "name": "%s"}' % name).encode()
        )
        await self.__writer.drain()

    def get_reader(self):
        return self.__reader

    def get_writer(self):
        return self.__writer
