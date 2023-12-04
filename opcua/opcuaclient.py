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
        json_data = {"packets": []}

        if (await node.read_data_type_as_variant_type() == VariantType.Boolean):
            if (isinstance(val, list)):
                # if val is a list, it's actually an array of booleans
                # asyncua doesn't have a specific type for that
                # right now we just support counting the number of
                # true entries as that's all we need here, but big TODO
                true_count = 0
                for bool_val in val:
                    if (bool_val):
                        true_count += 1
                val = true_count
            else:
                val = 1 if val else 0
        # TODO: investigate which, if any, other data types need support
        # here or if the rest is fine with implicit casts

        data_packet = {
            "name": "/".join((await node.get_path(as_string=True))[2:]),
            "value": val
        }
        # TODO: handle OPC methods; they have a val that is not serializable
        json_data["packets"].append(data_packet)
        # print("JSON", json_data)
        await self.__queue.put(json.dumps(json_data))

    def event_notification(self, event):
        print("New event", event)

    def __init__(self, queue):
        self.__queue = queue


class OpcuaClient:
    async def write_opcua(self):
        while True:
            data = json.loads(await self.__tcp_opc_queue.get())
            # print("tcp_opc:", data)
            try:
                node = await self.__client.nodes.objects.get_child(data["packets"][0]["name"].split("/"))
                value = data["packets"][0]["value"]
                data_variant_type = await node.read_data_type_as_variant_type()

                if (data_variant_type == VariantType.Boolean):
                    value = True if value else False
                elif (data_variant_type == VariantType.Double):
                    value = float(value)
                # TODO: support remaining needed data types

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
                    idx = await client.get_namespace_index(uri)
                    _logger.info("index of our namespace is %s", idx)

                handler = SubHandler(self.__opc_tcp_queue)
                self.__items = ua.CreateMonitoredItemsParameters()
                await self.load_opc_nodes(client.nodes.objects)
                # print("items", len(self.__items.ItemsToCreate))
                sub = await self.__client.create_subscription(100, handler)
                print("number of monitored items:", len(self.__items.ItemsToCreate))
                await sub.create_monitored_items(self.__items.ItemsToCreate)

                await asyncio.sleep(0.1)

                while True:
                    await asyncio.sleep(1)
            except Exception as e:
                print("[connect] exception:", e)

    async def load_opc_nodes(self, parent):
        try:
            children = await parent.get_children()
            if (len(children) == 0):
                # print("no further children")
                # print("id", parent.nodeid)
                if (not (parent.nodeid.Identifier == 6045 and parent.nodeid.NamespaceIndex == 4)):
                    # return
                    pass
                read_id = ua.ReadValueId()
                read_id.NodeId = parent.nodeid
                read_id.AttributeId = ua.AttributeIds.Value

                params = ua.MonitoringParameters()
                self.__item_id_counter += 1
                # needs to be before, because ClientHandle of 0 is not allowed
                params.ClientHandle = self.__item_id_counter

                # 0 means fastest sensible time as set by the server
                params.SamplingInterval = 0

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
                        pass
                        # print("Ignored blacklisted node:", child.__str__())
        except Exception as e:
            print("[load_opc_nodes] exception:", e)

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
