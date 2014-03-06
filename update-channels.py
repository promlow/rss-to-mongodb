import ConfigParser
import Queue
import threading
import feedparser
import hashlib
import os
import sys
from wsgiref.handlers import format_date_time
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
    def __init__(self, url, last_mod_date, etag, queue, event):
        self._url = url
        self._q = queue
        self._event = event
        self._etag = etag
        self._last_mod_date = last_mod_date

    def __call__(self):
        if self._last_mod_date and self._etag:
            items = feedparser.parse(self._url, etag=self._etag, modified=self._last_mod_date)
        elif self._last_mod_date:
            items = feedparser.parse(self._url, modified=self._last_mod_date)
        elif self._etag:
            items = feedparser.parse(self._url, etag=self._etag)
        else:
            items = feedparser.parse(self._url)

        try:
            etag = items.etag
        except AttributeError:
            etag = None

        mod_date = None
        try:
            mod_date = format_date_time(mktime(items.modified_parse))
        except AttributeError:
            if not etag:
                mod_date = format_date_time(mktime(datetime.utcnow().timetuple()))

        for entry in items.entries:
            item = {}
            if mod_date:
                item['modified'] = mod_date
            if etag:
                item['etag'] = items.etag
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


def _gen_guid(item):
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


class ItemInserter:
    def __init__(self, host, port, db_name, item_coll, chan_coll, queue, finish_events):
        self._client = MongoClient(host, port)
        self._db = client[db_name]
        self._items = self._db[item_coll]
        self._channels = self._db[chan_coll]
        self._q = queue
        self._channel_refs = {}
        self._finished = finish_events
        self._fetch_timestamp = datetime.utcnow().isoformat()

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

    def store_to_mongo(self, item):
        channel = self._channels.find_one({'url': item['url']})
        if channel:

            if 'last_fetched' in channel:
                if channel['last_fetched'] != self._fetch_timestamp:
                    channel['last_fetched'] = self._fetch_timestamp
            else:
                channel['last_fetched'] = self._fetch_timestamp

            if 'last_modified' in channel and 'modified' in item:
                if channel['last_modified'] != item['modified']:
                    channel['last_modified'] = item['modified']
            elif 'etag' in channel and 'etag' in item:
                if channel['etag'] != item['etag']:
                    channel['etag'] = item['etag']
            elif 'modified' in item:
                channel['last_modified'] = item['modified']
            elif 'etag' in item:
                channel['etag'] = item['etag']

            self._channels.save(channel)

            if len(item['guid']) < 1:
                item['guid'] = _gen_guid(item)

            db_item = self._items.find_one({'guid': item['guid']})
            if db_item:
                #print "skipping ", item['guid'], " found ", db_item['guid']
                return
            else:
                if item['url'] in self._channel_refs:
                    item['channel'] = self._channel_refs[item['url']]
                else:
                    item['channel'] = DBRef('channels', channel['_id'], self._db.name)
                    self._channel_refs[item['url']] = item['channel']

                del item['url']
                self._items.save(item)
        else:
            print "Failed to find channel with URL: %s" % item['url']


if __name__ == '__main__':
    import multiprocessing

    config = ConfigParser.SafeConfigParser({'host': 'localhost', 'port': '27017', 'db': 'feed_reader'})
    config.read('mongo.cfg')
    host = config.get('mongodb', 'host')
    port = config.getint('mongodb', 'port')
    db_name = config.get('mongodb', 'db')

    client = MongoClient(host, port)
    db = client[db_name]
    channels = list(db.channels.find())
    client.disconnect()  # probably not the most efficient, but the mongo client isn't thread safe

    work_queue = Queue.Queue()
    finish_events = []
    tp = ThreadPool(multiprocessing.cpu_count(), queue_size=0, wait_timeout=1)
    ii = ItemInserter(host, port, db_name, 'items', 'channels', work_queue, finish_events)

    for channel in channels:

        mod_date = None
        try:
            etag = channel['etag']
        except KeyError:
            etag = None

        try:
            mod_date = channel['last_modified']
        except KeyError:
            mod_date = None

        e = threading.Event()
        fp = ChannelFetcherParser(channel['url'], mod_date, etag, work_queue, e)
        ii._finished.append(e)
        tp.addTask(fp)

    tp.addTask(ii)
    work_queue.join()
    tp.cleanUpThreads()

