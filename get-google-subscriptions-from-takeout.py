from zipfile import ZipFile

from xml.dom import minidom

import ConfigParser
import sys
import feedparser

from pymongo import MongoClient
from bson.dbref import DBRef


def get_tags(node, tags):
    if node.hasChildNodes():
        i = 0
        tag = ""
        while i < node.attributes.length:
            nnm = node.attributes.item(i)
            if nnm.name == 'text':
                tag = nnm.value
                if not nnm.value in tags:
                    tags[tag] = []
            i += 1

        for n in node.childNodes:
            if n.nodeType == n.ELEMENT_NODE:
                if n.hasAttributes():
                    i = 0
                    while i < n.attributes.length:
                        nnm = n.attributes.item(i)
                        if nnm.name == 'xmlUrl':
                            val = nnm.value
                            if val not in tags[tag]:
                                tags[tag].append(val)
                        i += 1


def parse_xml(xml, subscripts):
    dom = minidom.parseString(xml)
    subs = dom.getElementsByTagName('outline')
    sn = 0
    for sub in subs:
        i = 0
        attrs = {}
        while i < sub.attributes.length:
            nnm = sub.attributes.item(i)
            attrs[nnm.name] = nnm.value
            i += 1

        if 'xmlUrl' in attrs:
            sn += 1
            subscripts.append(attrs)
        else:
            get_tags(sub, tags)

    return subscripts


def tag_subs(subs, tags):
    for feed in subs:
        xmlurl = feed['xmlUrl']
        for tag in tags:
            for tagurl in tags[tag]:
                if xmlurl == tagurl:
                    if not 'tags' in feed:
                        feed['tags'] = []
                    feed['tags'].append(tag)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Supply a path to the takeout file, please'
    else:
        subscripts = None
        tags = {}
        takeout = ZipFile(sys.argv[1])
        for _file in takeout.infolist():
            sub_file = None
            if _file.filename.endswith('subscriptions.xml'):
                sub_file = _file.filename
                print "found subscriptions file: ", sub_file

            if sub_file:
                ext_file = takeout.open(sub_file, 'rU')
                xml = ""
                for l in ext_file:
                    xml = xml + l

                subscripts = parse_xml(xml, [])

        print "Preparing to insert %d feeds" % len(subscripts)
        tag_subs(subscripts, tags)

        config = ConfigParser.SafeConfigParser({'host': 'localhost', 'port': '27017', 'db': 'feed_reader'})
        config.read('mongo.cfg')
        host = config.get('mongodb', 'host')
        port = config.get('mongodb', 'port')
        db_name = config.get('mongodb', 'db')

        #set some sane defaults, if the entry is 'host=', e.g.
        if len(host) == 0:
            host = 'localhost'

        if len(port) == 0:
            port = 27017
        else:
            port = int(port)

        if len(db_name) == 0:
            db_name = 'feed_reader'

        nick = config.get('user', 'nick')
        if len(nick) == 0:
            nick = 'test_nickname'

        email = config.get('user', 'email')
        if len(email) == 0:
            email = 'test@example.com'

        client = MongoClient(host, port)  # connect to server
        db = client[db_name]  # get a database
        db_channels = db.channels  # get a table/collection

        db_channels.remove()  # always remove existing

        #insert into channels
        channels = []
        for sub in subscripts:
            channel = {'url': sub['xmlUrl']}
            #fetch channel info
            print "fetching: ", channel['url']
            c = feedparser.parse(channel['url'])
            feed = c.feed
            channel['title'] = feed.title
            channel['link'] = feed.link
            channel['description'] = feed.description
            channel['language'] = feed.get('language', '')
            if 'icon' in feed:
                channel['image'] = feed.icon.href
            elif 'image' in feed:
                channel['image'] = feed.image.href

            chan_id = db_channels.save(channel)
            channels.append(DBRef('channels', chan_id, db_name))

        users = db.users
        user = users.find_one({'email': email})
        if not user:
            print "creating user {0} with nickname: {1}".format(email, nick)
            user = users.insert({'email': email, 'nick': nick})

        user['subscriptions'] = channels
        users.save(user)
        print "User {0} has {1} subscriptions".format(user['email'], len(user['subscriptions']))