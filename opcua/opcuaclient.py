import logging
import asyncio
from datetime import datetime

from asyncua import Client
from asyncua.ua.uatypes import VariantType as VariantType
from asyncua.ua.uatypes import DataValue as DataValue
from asyncua.common import ua_utils
import json

_logger = logging.getLogger("root")

class SubHandler(object):
    """
    Subscription Handler. To receive events from server for a subscription
    data_change and event methods are called directly from receiving thread.
    Do not do expensive, slow or network operation there. Create another
    thread if you need to do such a thing
    """

    async def datachange_notification(self, node, val, data):
        print("New data change event", node, val, data)
        json_data = { "packets": [] }
        print("data type", await node.read_data_type_as_variant_type())
        if (await node.read_data_type_as_variant_type() == VariantType.Boolean):
            val = 1 if val else 0
        data_packet = {"name": "/".join((await node.get_path(as_string=True))[2:]) , "value": val}
        json_data["packets"].append(data_packet)
        await self.__queue.put(json.dumps(json_data))
        print("queue size:", self.__queue.qsize())


    def event_notification(self, event):
        print("New event", event)

    def __init__(self, queue):
        self.__queue = queue

class OpcuaClient:
    async def write_opcua(self):
        while True:
            data = json.loads(await self.__tcp_opc_queue.get())
            print("tcp_opc:", data)
            try:
                node = await self.__client.nodes.objects.get_child(data["packets"][0]["name"].split("/"))
                value = data["packets"][0]["value"]
                data_variant_type = await node.read_data_type_as_variant_type()
                if (data_variant_type == VariantType.Boolean):
                    value = True if value else False
                print("trying to write", value, data_variant_type)
                opc_value = DataValue(int(value), data_variant_type)
                #opc_value.SourceTimestamp = 0 #datetime.now()
                await node.set_data_value(opc_value)
            except Exception as e:
                print("exception during writing to opcua:", e)

    async def connect(self, serverUrl):
        async with Client(url=serverUrl) as client:
            try:
                self.__client = client
                _logger.info("\n\n-----\n")
                _logger.info("Root node is: %r", client.nodes.root)
                _logger.info("Objects node is: %r", client.nodes.objects)

                #uri = "http://examples.freeopcua.github.io"
                uri = "http://auto.tuwien.ac.at/iot-lab/Node-RED/01_Distribution"
                idx = await client.get_namespace_index(uri)
                _logger.info("index of our namespace is %s", idx)

                # get a specific node knowing its node id
                #var = client.get_node(ua.NodeId(1002, 2))
                #var = client.get_node("ns=3;i=2002")
                #print(var)
                #await var.read_data_value() # get value of node as a DataValue object
                #await var.read_value() # get value of node as a python builtin
                #await var.write_value(ua.Variant([23], ua.VariantType.Int64)) #set node value using explicit data type
                #await var.write_value(3.9) # set node value using implicit data type

                # Now getting a variable node using its browse path
                #_logger.info("load opc structure")
                handler = SubHandler(self.__opc_tcp_queue)
                await self.load_opc_structure(handler, client.nodes.objects)
                _logger.info("\n\nhello %r\n\n", self.__opc_structure)
                #_logger.info("done")
                #myvar = await client.nodes.root.get_child(["0:Objects", "2:MyObject", "2:MyVariable"])
                #obj = await client.nodes.root.get_child(["0:Objects", "2:MyObject"])
                myvar = await client.nodes.objects.get_child(["4:01_Distribution", "4:Indicators", "4:Lights", "4:Red"])
                _logger.info("myvar is: %r", myvar)

                # subscribing to a variable node
                sub = await client.create_subscription(1, handler)
                handle = await sub.subscribe_data_change(myvar)
                await asyncio.sleep(0.1)

                # we can also subscribe to events from server
                await sub.subscribe_events()
                # await sub.unsubscribe(handle)
                # await sub.delete()

                # calling a method on server
                #res = await obj.call_method("2:multiply", 3, 2)
                #_logger.info("method result is: %r", res)
                while True:
                    await asyncio.sleep(1)
            except Exception as e:
                print("exception:", e)

    async def load_opc_structure(self, sub_handler, parent):
        try:
            children = await parent.get_children()
            if (len(children) == 0):
                #print("no further children")
                if ((await parent.read_browse_name()).Name == "Red" or
                    (await parent.read_browse_name()).Name == "Product"):
                    print ("subscribing to", (await parent.read_browse_name()).Name)
                    sub = await self.__client.create_subscription(10, sub_handler)
                    await sub.subscribe_data_change(parent)
                    #self.__sub_count += 1
                    #print (self.__sub_count)
            else:
                #print("children size:", len(children))
                for child in children:
                    #print("  -- child", child)
                    if (not child.__str__() in self.__node_blacklist):
                        await self.load_opc_structure(sub_handler, child)
                    else:
                        print("Ignored blacklisted node:", child.__str__())
        except Exception as e:
            print("exception:", e)


    def __init__(self, opc_tcp_queue, tcp_opc_queue):
        self.__sub_count = 0
        self.__node_blacklist = ["ns=2;i=5001", "i=2253", "ns=3;s=PLC"]
        self.__opc_structure = {
            "objects": {}
        }
        self.__opc_tcp_queue = opc_tcp_queue
        self.__tcp_opc_queue = tcp_opc_queue
