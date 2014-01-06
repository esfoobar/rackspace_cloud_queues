from cloud_queues import *

""" Test """
username = RCQ_USERNAME
apikey = RCQ_APIKEY
url = RCQ_URL_ENDPOINT

""" create a Producer instance """
pub = Producer(url, username, apikey)
pub.queue_name = 'testqueue'

if not pub.queue_exists():
    print "Creating queue", pub.queue_name
    pub.create_queue({"metadata": "My Queue"})
else:
    print "Queue exists", pub.queue_name

""" create and post two messages """
data = [{"ttl": 60,"body": {"task":"one"}},{"ttl": 60,"body": {"task":"two"}}]
for message in pub.post_messages(data):
    print "message: ", message

""" create a Consumer instance """
con = Consumer(url, username, apikey)

""" define ttl and grace times for the claim """
data = {"ttl":60, "grace":60}
con.queue_name = 'testqueue'
messages = con.claim_messages(data, 2)
for message in messages:
    print "task: ", message['body']['task']
    print 'message href:', message['href']

"""
do something with the messages
when done delete
"""

for message in messages:
    con.delete_message(message['href'])
