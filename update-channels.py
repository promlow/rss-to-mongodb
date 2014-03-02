import ConfigParser
import Queue
import threading
import feedparser
import hashlib
import os
import sys

from datetime import datetime
from time import mktime
from pymongo import MongoClient
from bson.dbref import DBRef

smodcfg = ConfigParser.SafeConfigParser()
smodcfg.read('submodule.cfg')
ptp = smodcfg.get('threadpool', 'dir')
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), ptp)))

from threadpool import ThreadPool


class ChannelFetcherParser:
    
    def __init__(self, url, queue, event):
        self._url = url
        self._q = queue
        self._event = event

    def __call__(self):
        items = feedparser.parse(self._url)
        for entry in items.entries:
            item = {}
            item['url'] = self._url
            item['title'] = entry.get('title', '')
            item['link'] = entry.get('link', '')
            item['summary'] = entry.get('summary', '')
            item['description'] = entry.get('description', '')
            item['content'] = entry.get('content', [{}])
            #http://pythonhosted.org/feedparser/common-atom-elements.html
            #Atom entries can have more than one content element,
            # content is a list of dictionaries
            item['author'] = entry.get('author', '')
            item['guid'] = entry.get('guid', '')
            if 'published_parsed' in entry:
                item['date'] = datetime.fromtimestamp(mktime(entry['published_parsed'])).isoformat()
            else:
                item['date'] = datetime.utcnow().isoformat()
                item['made_up_date'] = True

            self._q.put(item, block=True)

        self._event.set()
            
            
class ItemInserter:

    def __init__(self, host, port, db_name, item_coll, chan_coll, queue, finish_events):
        self._client = MongoClient(host, port)
        self._db = client[db_name]
        self._items = self._db[item_coll]
        self._channels = self._db[chan_coll]
        self._q = queue
        self._channel_refs = {}
        self._finished = finish_events

    
    def __del__(self):
        self._client.disconnect()

    def all_tasks_completed(self):
        for event in self._finished:
            if not event.is_set():
                return False

        return True

    def __call__(self):
        while True:
            if self._q.empty() and self.all_tasks_completed():
                if self._q.empty():
                    break

            try:
                item = self._q.get(block=True, timeout=1)
            except Queue.Empty:
                continue
            else:
                try:
                    self.store_to_mongo(item)
                finally:
                    self._q.task_done()

    def _gen_guid(self, item):
        h = hashlib.sha256()

        if not 'made_up_date' in item:
            h.update(str(item['date']))
            
            ba = bytearray(item['title'], 'utf-8')
            h.update(ba)
            ba = bytearray(item['link'], 'utf-8')
            h.update(ba)
            ba = bytearray(item['summary'], 'utf-8')
            h.update(ba)
            ba = bytearray(item['description'], 'utf-8')
            h.update(ba)
            ba = bytearray(item['author'], 'utf-8')
            h.update(ba)
            for d in item['content']:
                for k in d:
                    if d[k]:
                        ba = bytearray(d[k], 'utf-8')
                        h.update(ba)
        return h.hexdigest()


    def store_to_mongo(self, item):
        channel = self._channels.find_one({'url' : item['url']})
        if channel:
            if len(item['guid']) < 1:
                item['guid'] = self._gen_guid(item)

            db_item = self._items.find_one({'guid' : item['guid']})
            if db_item: 
                #print "skipping ", item['guid'], " found ", db_item['guid']
                return
            else:
                if item['url'] in self._channel_refs:
                    item['channel'] = self._channel_refs[item['url']]                    
                else:
                    channel['last_fetched'] = datetime.utcnow().isoformat() #first accessing this channel
                    item['channel'] = DBRef('channels', channel['_id'], self._db.name)
                    self._channel_refs[item['url']] = item['channel']
                    
                del item['url']
                channel['last_updated'] = datetime.utcnow().isoformat()
                self._items.save(item)
            self._channels.save(channel)
        else:
            print "Failed to find channel with URL: %s" % item['url']    
        

if __name__ == '__main__':
    import multiprocessing
    
    config = ConfigParser.SafeConfigParser({'host' :'localhost', 'port' : '27017', 'db' : 'feed_reader'})
    config.read('mongo.cfg')
    host = config.get('mongodb', 'host')
    port = config.getint('mongodb', 'port')
    db_name = config.get('mongodb', 'db')
    
    client = MongoClient(host, port)
    db = client[db_name]
    channels = list(db.channels.find())
    client.disconnect() #probably not the most efficient, but the mongo client isn't thread safe

    work_queue = Queue.Queue()
    finish_events = []
    tp = ThreadPool(multiprocessing.cpu_count(), queue_size=0, wait_timeout=1)
    ii = ItemInserter(host, port, db_name, 'items', 'channels', work_queue, finish_events)     

    for channel in channels:
        e = threading.Event()
        fp = ChannelFetcherParser(channel['url'], work_queue, e)
        ii._finished.append(e)
        tp.addTask(fp)

    tp.addTask(ii)
    work_queue.join()
    tp.cleanUpThreads()

