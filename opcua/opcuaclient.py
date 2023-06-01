import logging
import asyncio

from asyncua import Client
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
        node_id = "testSolenoid1" #hardcoded for now
        json_data = { "packets": [] }
        data_packet = {"name": node_id , "value": val}
        json_data["packets"].append(data_packet)
        await self.__queue.put(json.dumps(json_data))
        print("New data change event", node, val, data)
        print("queue size:", self.__queue.qsize())


    def event_notification(self, event):
        print("New event", event)

    def __init__(self, queue):
        self.__queue = queue

class OpcuaClient:
    async def connect(self, serverUrl):
        async with Client(url=serverUrl) as client:
            _logger.info("Root node is: %r", client.nodes.root)
            _logger.info("Objects node is: %r", client.nodes.objects)

            # Node objects have methods to read and write node attributes as well as browse or populate address space
            _logger.info("Children of root are: %r", await client.nodes.root.get_children())

            uri = "http://examples.freeopcua.github.io"
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
            myvar = await client.nodes.root.get_child(["0:Objects", "2:MyObject", "2:MyVariable"])
            obj = await client.nodes.root.get_child(["0:Objects", "2:MyObject"])
            _logger.info("myvar is: %r", myvar)

            # subscribing to a variable node
            handler = SubHandler(self.__queue)
            sub = await client.create_subscription(10, handler)
            handle = await sub.subscribe_data_change(myvar)
            await asyncio.sleep(0.1)

            # we can also subscribe to events from server
            await sub.subscribe_events()
            # await sub.unsubscribe(handle)
            # await sub.delete()

            # calling a method on server
            res = await obj.call_method("2:multiply", 3, 2)
            _logger.info("method result is: %r", res)
            while True:
                await asyncio.sleep(1)

    def __init__(self, queue):
        self.__queue = queue