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
    
    queues = {}
    bmp_map = {}
    offset_update_freq = config.offset_update_freq

    for host in config.bagheera_nodes:
        for topic in config.topics:
            for partition in config.partitions:
                queue = Queue.Queue(256)
                queues[(host, topic, partition)] = queue

                bmp = BagheeraMessageProcessor(queue)
                bmp_map[id(bmp)] = (host, topic, partition)

                offset = offsets[(host, topic, partition)]
                kc = KafkaConsumer(host, {}, topic, partition, bmp.processor, offset, offset_update_freq)
                t = threading.Thread(target = kc.process_messages_forever)
                t.start()

    while True:
        for htp, q in queues.iteritems():
            try:
                v = q.get(False)
            except Queue.Empty:
                continue

            if v[1] == 'PUT':
                pid, op, ts, ipaddr, doc_id, payload = v
                System.out.println('%s %s %d %s %s %s' % (htp[1], op, ts, ipaddr, doc_id, payload))

            elif v[1] == 'DELETE':
                pid, op, ts, ipaddr, doc_id = v
                System.out.println('%s %s %d %s %s' % (htp[1], op, ts, ipaddr, doc_id))


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
            
    if (not offsets) or (len(offsets) != (len(config.topics) * len(config.partitions) * len(config.bagheera_nodes))):
        System.err.println("ERROR: could not find valid initial offsets for given configuration")
        sys.exit(1)

    return offsets


if __name__ == '__main__':
    if len(sys.argv) != 2:
        System.err.println("Needs file containing offsets as first argument")
        sys.exit(1)
    
    try:
        runner(parse_offsets(sys.argv[1]))
    except:
        System.err.println("ERROR: " + traceback.format_exc())
    finally:
        System.exit(1)


