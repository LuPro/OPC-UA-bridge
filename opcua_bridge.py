import argparse
import asyncio
import logging

from tcpsocket.tcpsocket import TcpSocket
from opcua.opcuaclient import OpcuaClient
from forwarder.forwarder import Forwarder

_logger = logging.getLogger("root")

async def socket_forwarder(reader, writer):
    queue = asyncio.Queue()

    async def read_and_forward():
        while True:
            data = (await reader.readuntil(b"\0")).decode()
            if not data:
                _logger.error("Reading from socket gave no data")
                break
            await queue.put(data)

    async def forward():
        while True:
            data = await queue.get()
            writer.write(data)
            await writer.drain()

    await asyncio.gather(read_and_forward(), forward())

def read_arguments(args):
    return args

async def main():
    arg_parser = argparse.ArgumentParser(description="todo")
    arg_parser.add_argument("data", metavar="IN_DATA", nargs="?", help="Enter the OPC UA endpoint to connect to")

    args = arg_parser.parse_args()

    tcpsocket = TcpSocket()
    # asyncio.gather(
    #     tcpsocket.connect()
    # )
    await tcpsocket.connect()
    #I'd like to set this up before connecting tcp, but before connecting I have no stream reader/writer
    forwarder = Forwarder(tcpsocket.get_reader(), tcpsocket.get_writer())

    opcua = OpcuaClient(forwarder.get_opcua_tcp_queue(), forwarder.get_tcp_opcua_queue())
    asyncio.gather(
        #opcua.connect("opc.tcp://172.21.55.10:4840"),
        opcua.connect("opc.tcp://0.0.0.0:4840/freeopcua/server/"),
        forwarder.read_tcp(),
        forwarder.write_tcp(),
        opcua.write_opcua()
    )

    while True:
        await asyncio.sleep(1)

    # this should work for two socket based streams, but I doubt I can hook up the opcua connection like this
    # will probably need to have a custom opcua.reader function that puts stuff in the queue and a custom opcua.writer
    # await asyncio.gather(
    #     socket_forwarder(tcpReader, opcuaWriter),
    #     socket_forwarder(opcuaReader, tcpWriter)
    # )

if __name__ == "__main__":
    #logging.basicConfig(level=logging.INFO)
    asyncio.run(main())

