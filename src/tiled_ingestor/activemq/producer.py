import stomp
import json
import sys

conn = stomp.Connection([("k11-control", 61613)],auto_content_length=False)
# conn.start()
conn.connect()
print('Connected!')

# replace thhe string below with the desired filepatha
filepath = "//dls/k11/data/2024/mg37376-1/processing/20240311120527_37086/k11-37086_processed.nxs"
message = json.dumps({'filePath': filepath})
destination = '/topic/org.dawnsci.file.topic'
conn.send(destination, message, ack='auto')

print('Exiting...')
sys.exit(1)