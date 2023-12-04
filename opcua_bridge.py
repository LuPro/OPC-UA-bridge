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
    arg_parser.add_argument("-f", "--frontend", metavar="FRONTEND", help="Enter the adress and port of the frontend to connect to. If left empty defaults to \"localhost:3000\"")
    arg_parser.add_argument("-b", "--backend", metavar="BACKEND", help="Enter the OPC UA endpoint to connect to")
    arg_parser.add_argument("-n", "--name", metavar="NAME", help="Enter the name that the bridge reports to frontend. If left empty defaults to \"OPC Backend\".")

    args = arg_parser.parse_args()

    opcua_address = "opc.tcp://0.0.0.0:4840/freeopcua/server/"
    if (args.backend is not None):
        opcua_address = args.backend

    frontend_ip = "localhost"
    frontend_port = 3000
    if (args.frontend is not None):
        address_parts = args.frontend.split(":")
        frontend_ip = address_parts[0]
        frontend_port = address_parts[1]

    tcpsocket = TcpSocket()
    await tcpsocket.connect(frontend_ip, frontend_port, args.name)
    # I'd like to set this up before connecting tcp, but before connecting
    # I have no stream reader/writer
    forwarder = Forwarder(tcpsocket.get_reader(), tcpsocket.get_writer())

    opcua = OpcuaClient(forwarder.get_opcua_tcp_queue(), forwarder.get_tcp_opcua_queue())
    asyncio.gather(
        opcua.connect(opcua_address),
        forwarder.read_tcp(),
        forwarder.write_tcp(),
        opcua.write_opcua()
    )

    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    # logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
