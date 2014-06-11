
bagheera_nodes = [
    'host1',
    'host2',
    'host3'
    ]

topic = 'TOPIC'

partitions = [0]

# dump offsets after N records (per thread)
offset_update_freq = 1000

# kafka consumer connection params
DEFAULT_CONN_PARAMS = {
    'port' : 9092, # broker port
    'nrecs' : 10000, # batchsize per fetch
    'bufsize' : 10240000
    }
