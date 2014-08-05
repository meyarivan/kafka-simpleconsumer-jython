import os
import sys
import time

sys.path.extend(["lib/"+x for x in os.listdir("lib") if x.endswith('.jar')])

import jarray
from java.util import Map

from com.google.protobuf import ByteString
from com.mozilla.bagheera.BagheeraProto import BagheeraMessage

import com.alibaba.fastjson.JSON as JSON
import java.net.Inet4Address as Inet4Address
from java.lang import System


class BagheeraMessageProcessor:
    def __init__(self, queue):
        self.queue = queue

    def processor(self, msg):
        bmsg = BagheeraMessage.parseFrom(ByteString.copyFrom(msg.message().payload()))
        queue = self.queue
        ts = bmsg.getTimestamp()
        ip = Inet4Address.getByAddress(bmsg.getIpAddr().toByteArray()).getHostAddress()
        payload = bmsg.getPayload().toStringUtf8()
        doc_id = bmsg.getId()
        
        if bmsg.getOperation() == BagheeraMessage.Operation.CREATE_UPDATE:
            try:
                json_obj = JSON.parseObject(payload, Map)
            except:
                return

            queue.put((id(self), 'PUT', ts, ip, doc_id, json_obj))

            
        else:
            queue.put((id(self), 'DELETE', ts, doc_id, ip))
