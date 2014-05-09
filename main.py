import os
import sys
import time

from consumer import KafkaConsumer
from processor import BagheeraMessageProcessor

import Queue
import threading

import config

sys.path.extend(['log4j.properties'])

def runner():
    
    queues = []
    bmp_map = {}

    for host in config.bagheera_nodes:
        for part in config.partitions:
            queue = Queue.Queue(256)
            queues.append(queue)
            bmp = BagheeraMessageProcessor(queue)
            bmp_map[id(bmp)] = (host, part)
            kc = KafkaConsumer(host, {}, config.topic, part, bmp.processor)
            t = threading.Thread(target = kc.process_messages_forever)
            t.start()

    while True:
        for q in queues:
            try:
                pid, payload, ts, ip = q.get()
                print bmp_map[pid], ts, ip
            except Queue.Empty:
                continue



if __name__ == '__main__':
    runner()

