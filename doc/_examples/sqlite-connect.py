# sqlite-connect.py

import broker
import sys
import time

with broker.Endpoint() as ep, \
     ep.make_subscriber('/test') as s, \
     ep.make_status_subscriber(True) as ss:

    ep.peer('127.0.0.1', 9999, 1.0)

    st = ss.get();

    if not (type(st) == broker.Status and st.code() == broker.SC.PeerAdded):
        print('could not connect')
        sys.exit(1)

    c = ep.attach_clone('mystore')

    while True:
        time.sleep(1)
        c.increment('foo', 1)
        print(c.get('foo'))
