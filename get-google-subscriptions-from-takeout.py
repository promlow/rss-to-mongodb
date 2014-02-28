from zipfile import ZipFile

from xml.dom import minidom
from xml.dom import Node

import ConfigParser
import sys

tags = {}

def get_tags(node):
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
                config = ConfigParser.RawConfigParser()
                sn = 0
                for sub in subs:
                    i = 0
                    attrs = {}
                    while i < sub.attributes.length:
                        nnm = sub.attributes.item(i)
                        attrs[nnm.name] = nnm.value
                        i = i + 1

                        if 'xmlUrl' in attrs:
                            section = "subscription%d" % sn
                            config.add_section(section)
                            for key in iter(attrs):
                                config.set(section, key, attrs[key])
                                sn = sn + 1
                        else:
                            get_tags(sub)

                config.add_section('subscriptions')
                config.set('subscriptions', 'count', sn)

                i = 0
                config.add_section('tags')
                for tag in tags:
                    key = "tag%d" % i
                    config.set('tags', key, tag)
                    config.add_section(tag)
                    j = 0
                    for url in tags[tag]:
                        url_key = "url%d" % j
                        config.set(tag, url_key, url)
                        j = j + 1
                    i = i + 1
                
                
                with open('subs.cfg', 'wb') as configfile:
                    config.write(configfile)
                    
                
