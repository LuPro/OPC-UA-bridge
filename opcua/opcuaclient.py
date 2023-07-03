import logging
import asyncio

from asyncua import Client, ua
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
        # print("New data change event", node, val, data)
        print("New data change event", node, val)
        json_data = {"packets": []}

        # print("data type", await node.read_data_type_as_variant_type())
        if (await node.read_data_type_as_variant_type() == VariantType.Boolean):
            val = 1 if val else 0
        # TODO: investigate which if any other data types need to be supported
        # here or if the rest is fine with implicit casts

        data_packet = {
            "name": "/".join((await node.get_path(as_string=True))[2:]),
            "value": val
        }
        # TODO: handle OPC methods since they have a different val that is not serializable
        json_data["packets"].append(data_packet)
        print("JSON data", json_data)
        await self.__queue.put(json.dumps(json_data))
        # print("queue size:", self.__queue.qsize())

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
                elif (data_variant_type == VariantType.Double):
                    value = float(value)
                # TODO: support remaining needed data types

                # await node.write_value(ua.Variant(value, data_variant_type))
                await node.write_value(
                    ua.DataValue(ua.Variant(value, data_variant_type))
                )
            except Exception as e:
                print("exception during writing to opcua:", e)

    async def connect(self, serverUrl):
        async with Client(url=serverUrl) as client:
            # need to wrap all with a try except because all unhandled
            # exceptions in any of the OPC functions will just cause
            # freeopcua library client to disconnect without any visible error
            try:
                is_mock_server = False
                if (serverUrl == "opc.tcp://0.0.0.0:4840/freeopcua/server/"):
                    is_mock_server = True

                self.__client = client
                _logger.info("\n\n-----\n")
                _logger.info("Root node is: %r", client.nodes.root)
                _logger.info("Objects node is: %r", client.nodes.objects)

                if (is_mock_server):
                    uri = "http://examples.freeopcua.github.io"
                else:
                    uri = "http://auto.tuwien.ac.at/iot-lab/Node-RED/01_Distribution"
                idx = await client.get_namespace_index(uri)
                _logger.info("index of our namespace is %s", idx)

                # get a specific node knowing its node id
                # var = client.get_node(ua.NodeId(1002, 2))
                # var = client.get_node("ns=3;i=2002")
                # print(var)
                # await var.read_data_value() # get value of node as a DataValue object
                # await var.read_value() # get value of node as a python builtin
                # await var.write_value(ua.Variant([23], ua.VariantType.Int64)) #set node value using explicit data type
                # await var.write_value(3.9) # set node value using implicit data type

                # Now getting a variable node using its browse path
                handler = SubHandler(self.__opc_tcp_queue)
                self.__items = ua.CreateMonitoredItemsParameters()
                await self.load_opc_nodes(client.nodes.objects)
                print("items", len(self.__items.ItemsToCreate))
                sub = await self.__client.create_subscription(10, handler)
                await sub.create_monitored_items(self.__items.ItemsToCreate)
                # _logger.info("done")
                if (is_mock_server):
                    myvar = await client.nodes.root.get_child(["0:Objects", "2:MyObject", "2:MyVariable"])
                    obj = await client.nodes.root.get_child(["0:Objects", "2:MyObject"])
                else:
                    myvar = await client.nodes.objects.get_child(["4:01_Distribution", "4:Indicators", "4:Lights", "4:Red"])
                _logger.info("myvar is: %r", myvar)

                await asyncio.sleep(0.1)

                # we can also subscribe to events from server
                # await sub.subscribe_events()
                # await sub.unsubscribe(handle)
                # await sub.delete()

                # calling a method on server
                # res = await obj.call_method("2:multiply", 3, 2)
                # _logger.info("method result is: %r", res)
                while True:
                    await asyncio.sleep(1)
            except Exception as e:
                print("[connect] exception:", e)

    async def load_opc_nodes(self, parent):
        try:
            children = await parent.get_children()
            if (len(children) == 0):
                # print("no further children")
                read_id = ua.ReadValueId()
                read_id.NodeId = parent.nodeid
                read_id.AttributeId = ua.AttributeIds.Value

                params = ua.MonitoringParameters()
                params.ClientHandle = self.__item_id_counter
                self.__item_id_counter += 1

                item = ua.MonitoredItemCreateRequest()
                item.ItemToMonitor = read_id
                item.MonitoringMode = ua.MonitoringMode.Reporting
                item.RequestedParameters = params
                self.__items.ItemsToCreate.append(item)
            else:
                # print("children size:", len(children))
                for child in children:
                    # print("  -- child", child)
                    if (not child.__str__() in self.__node_blacklist):
                        await self.load_opc_nodes(child)
                    else:
                        print("Ignored blacklisted node:", child.__str__())
        except Exception as e:
            print("[load_opc_nodes] exception:", e)

    # async def load_opc_structure(self, sub_handler, parent):
    #     try:
    #         children = await parent.get_children()
    #         if (len(children) == 0):
    #             # print("no further children")
    #             if ((await parent.read_browse_name()).Name == "Red" or
    #                 (await parent.read_browse_name()).Name == "Product" or
    #                 (await parent.read_browse_name()).Name == "MyVariable" or True):
    #                 print ("subscribing to", (await parent.read_browse_name()).Name)
    #                 sub = await self.__client.create_subscription(10, sub_handler)
    #                 await sub.subscribe_data_change(parent)
    #                 self.__sub_count += 1
    #                 print (self.__sub_count)
    #         else:
    #             # print("children size:", len(children))
    #             for child in children:
    #                 # print("  -- child", child)
    #                 if (not child.__str__() in self.__node_blacklist):
    #                     await self.load_opc_structure(sub_handler, child)
    #                 else:
    #                     print("Ignored blacklisted node:", child.__str__())
    #     except Exception as e:
    #         print("exception:", e)

    def __init__(self, opc_tcp_queue, tcp_opc_queue):
        self.__item_id_counter = 0
        self.__sub_count = 0
        self.__node_list = []
        self.__node_blacklist = ["ns=2;i=5001", "i=2253", "ns=3;s=PLC"]
        self.__opc_structure = {
            "objects": {}
        }
        self.__opc_tcp_queue = opc_tcp_queue
        self.__tcp_opc_queue = tcp_opc_queue
