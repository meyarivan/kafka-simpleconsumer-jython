import os
import sys
import time

sys.path.extend(["lib/" + x for x in os.listdir("lib") if x.endswith('.jar')])

import jarray
from java.util import Map

from kafka.api import FetchRequest
from kafka.consumer import SimpleConsumer


class KafkaConsumer(object):
    DEFAULT_CONN_PARAMS = {
        'port' : 9092,
        'nrecs' : 10000,
        'bufsize' : 10240000
        }

    def __init__(self, hostname, conn_params, topic, partition, processor):
        self.hostname = hostname

        self.conn_params = {}
        self.conn_params.update(conn_params)
        self.conn_params.update(self.DEFAULT_CONN_PARAMS)

        self.topic = topic
        self.partition = partition

        self.processor = processor

    def process_messages_forever(self):
        consumer = SimpleConsumer(self.hostname, self.conn_params['port'], 
                                  self.conn_params['nrecs'], self.conn_params['bufsize'])

        offset =  long(consumer.getOffsetsBefore(self.topic, self.partition, long(time.time() * 1000), 3)[0]) #TODO
 
        while True:
            req = FetchRequest(self.topic, self.partition, offset, 1024 * 1024 * 64)
            messageset = consumer.fetch(req)
            for msg in messageset.elements():
                self.processor(msg)
                offset = msg.offset()

