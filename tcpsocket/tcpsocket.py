import logging
import asyncio

_logger = logging.getLogger("root")

class TcpSocket:
    async def __main(self):
        while True:
            #message = await self.__reader.read()
            message = f'{{"packets":[{{"name": "testSolenoid", "value": {self.__value}}}]}}'
            if self.__value == 0:
                self.__value = 1
            else:
                self.__value = 0
            #print("sending message", message)
            #self.__writer.write(message.encode())
            #await self.__writer.drain()
            #print("received message", await self.__reader.readuntil(b"\0"))
            await asyncio.sleep(1)

    async def connect(self, address = "localhost", port = 3000):
        self.__value = 0
        self.__reader, self.__writer = await asyncio.open_connection(address, port)
        #await self.__main()

    def get_reader(self):
        return self.__reader

    def get_writer(self):
        return self.__writer
