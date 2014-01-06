import requests
import json
import uuid

class Queue_Connection(object):

    def __init__(self, username, apikey):
        url = 'https://identity.api.rackspacecloud.com/v2.0/tokens'
        payload  = {"auth":{"RAX-KSKEY:apiKeyCredentials":{"username": username , "apiKey": apikey }}}
        headers = {'Content-Type': 'application/json'}
        r = requests.post(url, data=json.dumps(payload), headers=headers)
        self.token = r.json()['access']['token']['id']
        self.headers = {'X-Auth-Token' : self.token, 'Content-Type': 'application/json', 'Client-ID': uuid.uuid1(), 'X-Project-ID:': username}
        print "Token:", self.token
        print "Headers:", self.headers

    def token(self):
        return self.token

    def get(self, url, payload=None):
        r = requests.get(url, data=json.dumps(payload), headers=self.headers)
        return [r.status_code, r.headers, r.content]

    def post(self, url, payload=None):
        r = requests.post(url, data=json.dumps(payload), headers=self.headers)
        return [r.status_code, r.headers, r.content]

    def put(self, url, payload=None):
        r = requests.put(url, data=json.dumps(payload), headers=self.headers)
        return [r.status_code, r.headers, r.content]

    def delete(self, url, payload=None):
        r = requests.delete(url, data=json.dumps(payload), headers=self.headers)
        return [r.status_code, r.headers, r.content]


class Producer(Queue_Connection):

    def __init__(self, url, username, apikey):
        super(Producer, self).__init__(username, apikey)
        self.base_url = url

    def queue_name():
        def fget(self):
            return self._queue_name
        def fset(self, value):
            self._queue_name = value
        def fdel(self):
            del self._queue_name
        return locals()
    queue_name = property(**queue_name())


    def queue_exists(self):
        url = self.base_url + '/v1/queues/' + self.queue_name + '/stats'
        if self.get(url)[0] == 200:
            return True
        return False

    def create_queue(self, payload=None):
        url = self.base_url + "/v1/queues/" + self.queue_name
        res =  self.put(url, payload)
        print "Queue creation result code:" + str(res[0])
        if res[0] == 201:
            print '%s created' % self.queue_name
        elif res[0] == 204:
            print 'A queue named %s is present' % self.queue_name
        else:
            print 'Problem with queue creation,'

    def post_messages(self, payload):
        url = self.base_url + '/v1/queues/' + self.queue_name + '/messages'
        res = self.post(url, payload)
        print "Post message result code:" + str(res[0])
        if res[0] == 201:
            return json.loads(res[2])['resources']
        else:
            print "Couldn't post messages"

class Consumer(Queue_Connection):

    def __init__(self, url, username, apikey):
        super(Consumer, self).__init__(username, apikey)
        self.base_url = url

    def claim_messages(self, payload, limit=1):
        url = self.base_url + '/v1/queues/' + self.queue_name + '/claims?limit=' + str(limit)
        res = self.post(url, payload)
        print "Claim messages result code:" + str(res[0])
        if res[0] == 201:
            return json.loads(res[2])
        else:
            print "Couldn't claim messages"

    def delete_message(self, url):
        url = self.base_url + url
        res = self.delete(url)
        print "Delete messages result code:" + str(res[0])
        if res[0] == 204:
            print "Message deleted"

