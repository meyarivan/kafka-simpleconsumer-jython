import os
import sys
import time
import traceback
import json

from consumer import KafkaConsumer
from processor import BagheeraMessageProcessor

import Queue
import threading

import config
import codecs

sys.path.extend(['log4j.properties'])
sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

from java.lang import System


def runner(offsets):
    
    queues = []
    bmp_map = {}
    topic = config.topic
    offset_update_freq = config.offset_update_freq

    for host in config.bagheera_nodes:
        for partition in config.partitions:
            queue = Queue.Queue(256)
            queues.append(queue)
            bmp = BagheeraMessageProcessor(queue)
            bmp_map[id(bmp)] = (host, partition)

            offset = offsets[(host, topic, partition)]
            kc = KafkaConsumer(host, {}, topic, partition, bmp.processor, offset, offset_update_freq)
            t = threading.Thread(target = kc.process_messages_forever)
            t.start()

    while True:
        for q in queues:
            try:
                v = q.get(False)
            except Queue.Empty:
                continue

            if v[1] == 'PUT':
                pid, op, ts, ipaddr, payload = v
                System.out.println('%s %d %s %s' % (op, ts, ipaddr, payload))

            if v[1] == 'DELETE':
                pid, op, ts, ipaddr = v
                System.out.println('%s %d %s' % (op, ts, ipaddr))


def parse_offsets(filex):
    offsets = {}

    # lines in this "file" contain one serialized (json) entry per line with following fields
    # time_millis hostname topic partition offset
    #

    for i in open(filex, "r"):
        try:
            dictx = json.loads(i)
            host = dictx['hostname']
            topic = dictx['topic']
            partition = dictx['partition']
            offset = dictx['offset']

            offsets[(host, topic, partition)] = offset
        except:
            pass
            
    if (not offsets) or (len(offsets) != (len(config.partitions) * len(config.bagheera_nodes))):
        System.err.println("ERROR: could not find valid initial offsets for given configuration")
        sys.exit(1)

    return offsets


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print >> sys.stderr, "Needs file containing offsets as first argument"
        sys.exit(1)
    
        
    runner(parse_offsets(sys.argv[1]))
