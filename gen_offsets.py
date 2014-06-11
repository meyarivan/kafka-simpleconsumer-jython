import os
import sys
import time
import os.path
import json
import config
import json

sys.path.extend(["lib/"+x for x in os.listdir("lib") if x.endswith('.jar')])

from java.lang import System

from kafka.consumer import SimpleConsumer


#
# offsets_after_time_millis = -1 => get latest offset
# offsets_after_time_millis = -2 => get oldest offset
#

def get_offsets(offsets_after_time_millis,
                conn_params = config.DEFAULT_CONN_PARAMS):

    curr_time = long(time.time() * 1000)

    for host in config.bagheera_nodes:
        for partition in config.partitions:
            consumer = SimpleConsumer(host, conn_params['port'],
                                      conn_params['nrecs'], conn_params['bufsize'])

            offset =  long(consumer.getOffsetsBefore(config.topic, 
                                                     partition, 
                                                     offsets_after_time_millis, 1)[0])


            consumer.close()

            System.out.println(json.dumps({
                        'time_millis' : curr_time,
                        'hostname' : host, 
                        'topic' : config.topic,
                        'partition' : partition,
                        'offset' : offset
                        }))

                        

if __name__ == '__main__':
    get_offsets(-1)
