from zipfile import ZipFile

from xml.dom import minidom
from xml.dom import Node

import ConfigParser
import sys



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

            #ignoring groups for now
            if 'xmlUrl' in attrs:
                section = "subscription%d" % sn
                config.add_section(section)
                for key in iter(attrs):
                    config.set(section, key, attrs[key])
                sn = sn + 1

        config.add_section('subscriptions')
        config.set('subscriptions', 'count', sn)
                
        with open('subs.cfg', 'wb') as configfile:
            config.write(configfile)
