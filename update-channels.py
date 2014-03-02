import ConfigParser
import Queue
import threading
import feedparser

import os
import sys

from datetime import datetime
from pymongo import MongoClient
from bson.dbref import DBRef

smodcfg = ConfigParser.SafeConfigParser()
smodcfg.read('submodule.cfg')
ptp = smodcfg.get('threadpool', 'dir')
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), ptp)))

from threadpool import ThreadPool


class ChannelFetcherParser:
    
    def __init__(self, url, queue, lock):
        self._url = url
        self._q = queue
        self._lock = lock


    def __call__(self):
        self._lock.acquire() #assuming feedparser is not thread safe
        items = feedparser.parse(self._url)
        self._lock.release()
        #print self._url, len(items.entries)
        for entry in items.entries:
            now = datetime.utcnow()
            item = {}
            item['url'] = self._url
            item['title'] = entry.get('title', '')
            item['link'] = entry.get('link', '')
            item['summary'] = entry.get('summary', '')
            item['description'] = entry.get('description', '')
            item['content'] = entry.get('content', '')
            item['author'] = entry.get('author', '')
            item['guid'] = entry.get('guid', '')
            item['date'] = entry.get('published_parsed', now)
            if item['date'] == now:
                item['made_up_date'] = True

            self._q.put(item, block=True)

            
class ItemInserter:

    def __init__(self, host, port, db_name, item_coll, chan_coll, queue):
        client = MongoClient(host, port)
        self._db = client[db_name]
        self._items = self._db[item_coll]
        self._channels = self._db[chan_coll]
        self._q = queue
        self._channel_refs = {}
        self._finished = threading.Event()

    def __call__(self):
        finished = False
        while True:
            if self._finished.isSet():
                if finished:
                    break

            try:
                item = self._q.get(block=True, timeout=1)
            except Queue.Empty:
                if self._finished.isSet():
                    finished = True
                continue
            else:
                try:
                    self.store_to_mongo(item)
                finally:
                    self._q.task_done()


    def store_to_mongo(self, item):
        print item['url'], item['title']

        

if __name__ == '__main__':
    import multiprocessing
    
    work_queue = Queue.Queue()
    try:
        tp = ThreadPool(multiprocessing.cpu_count() * 3, queue_size=50, wait_timeout=1)
        
        lock = threading.RLock()
        for url in local_files:
            fp = ChannelFetcherParser(url, work_queue, lock)
            tp.addTask(fp)
            
        ii = ItemInserter('server', 27017, 'feed_reader', 'items', 'channels', work_queue)
        tp.addTask(ii)
    finally:
        work_queue.join()
        ii._finished.set()
        tp.cleanUpThreads()

