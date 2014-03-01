from zipfile import ZipFile

from xml.dom import minidom
from xml.dom import Node

import ConfigParser
import sys

from pymongo import MongoClient

def get_tags(node, tags):
    if node.hasChildNodes():
        i = 0
        tag = ""
        while i < node.attributes.length:
            nnm = node.attributes.item(i)
            if (nnm.name == 'text'):
                tag = nnm.value
                if not nnm.value in tags:
                    tags[tag] = []
            i = i + 1

        for n in node.childNodes:
            if n.nodeType == n.ELEMENT_NODE:
                if n.hasAttributes():
                    i = 0                    
                    while i < n.attributes.length:
                        nnm = n.attributes.item(i)
                        if (nnm.name == 'xmlUrl'):
                            val = nnm.value
                            if val not in tags[tag]:
                                tags[tag].append(val)
                        i = i + 1
                                

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Supply a path to the takeout file, please'
    else:
        subscripts = []
        tags = {}
        takeout = ZipFile(sys.argv[1])
        for _file in takeout.infolist():
            if _file.filename.endswith('subscriptions.xml'):
                sub_file = _file.filename
                print "found subscriptions file: ", sub_file

            if sub_file:
                ext_file = takeout.open(sub_file, 'rU')
                xml = ""
                for l in ext_file:
                    xml = xml + l

                dom = minidom.parseString(xml)
                subs = dom.getElementsByTagName('outline')
                sn = 0
                for sub in subs:
                    i = 0
                    attrs = {}
                    while i < sub.attributes.length:
                        nnm = sub.attributes.item(i)
                        attrs[nnm.name] = nnm.value
                        i = i + 1

                    if 'xmlUrl' in attrs:
                        sn = sn + 1
                        subscripts.append(attrs)
                    else:
                        get_tags(sub, tags)
        print "Sub count %d" % sn

    print "len(tags) %d" % len(tags)
    print "Preparing to insert %d feeds" % len(subscripts)
    for feed in subscripts:
        feed_tags = []
        xmlurl = feed['xmlUrl']
        for tag in tags:
            for tagurl in tags[tag]:
                if xmlurl == tagurl:
                    if 'tags' in feed:
                        feed['tags'].append(tag)
                    else:
                        feed['tags'] = []
                        feed['tags'].append(tag)



    config = ConfigParser.ConfigParser()
    config.read('mongo.cfg')
    url = config.get('mongodb', 'url')
    client = MongoClient(url) #connect to server
    db = client.feed_reader   #get a database
    feeds = db.feeds          #get a table
    
    count = feeds.insert(subscripts)
    
    
    print "Inserted %d feeds" % len(count)

                
