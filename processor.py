import os
import sys
import time

sys.path.extend(["lib/" + x for x in os.listdir("lib") if x.endswith('.jar')])

import jarray
from java.util import Map
import java.net.InetAddress as InetAddress

from com.google.protobuf import ByteString
from com.mozilla.bagheera.BagheeraProto import BagheeraMessage


class BagheeraMessageProcessor:
    def __init__(self, queue):
        self.queue = queue

    def processor(self, msg):
        bmsg = BagheeraMessage.parseFrom(ByteString.copyFrom(msg.message().payload()))
        queue = self.queue

        if bmsg.getOperation() == BagheeraMessage.Operation.CREATE_UPDATE:
            try:
                payload = bmsg.getPayload().toStringUtf8()
                ip = InetAddress.getByAddress(bmsg.getIpAddr().toByteArray())
                ts = bmsg.getTimestamp()

                queue.put((id(self), payload, ts, ip))
            except:
                return

            

