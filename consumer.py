import os
import sys
import time
import os.path
import json
import config

sys.path.extend(["lib/"+x for x in os.listdir("lib") if x.endswith('.jar')])

import jarray
from java.util import Map
from java.lang import System

from kafka.api import FetchRequest
from kafka.consumer import SimpleConsumer


class KafkaConsumer(object):
    DEFAULT_CONN_PARAMS = config.DEFAULT_CONN_PARAMS

    def __init__(self, hostname, conn_params, topic, partition, 
                 processor, offset, offset_update_freq):
        self.hostname = hostname

        self.conn_params = {}
        self.conn_params.update(conn_params)
        self.conn_params.update(self.DEFAULT_CONN_PARAMS)

        self.topic = topic
        self.partition = partition
        self.processor = processor

        self.offset_update_freq = long(offset_update_freq)
        self.offset = offset

    def update_offset(self, offset):
        System.err.println(json.dumps({
                    'time_millis' : System.currentTimeMillis(),
                    'hostname' : self.hostname, 
                    'topic' : self.topic,
                    'partition' : self.partition,
                    'offset' : offset
                    }))

    def process_messages_forever(self):
        consumer = SimpleConsumer(self.hostname, self.conn_params['port'], 
                                  self.conn_params['nrecs'], self.conn_params['bufsize'])

        if self.offset is None:
            offset =  long(consumer.getOffsetsBefore(self.topic, 
                                                     self.partition, 
                                                     long(time.time() * 1000), 3)[0]) #TODO
        else:
            offset = self.offset

        nrecs = 0

        while True:
            req = FetchRequest(self.topic, self.partition, offset, 1024 * 1024 * 64)
            messageset = consumer.fetch(req)
            for msg in messageset.elements():
                self.processor(msg)
                offset = msg.offset()

                nrecs += 1
                if nrecs >= self.offset_update_freq:
                    self.update_offset(offset)
                    nrecs = 0
