# sqlite-listen.py

import broker

with broker.Endpoint() as ep, \
     ep.make_subscriber('/test') as s, \
     ep.make_status_subscriber(True) as ss:

    ep.listen('127.0.0.1', 9999)

    m = ep.attach_master('mystore',
                         broker.Backend.SQLite, {'path': 'mystore.sqlite'})

    while True:
        print(ss.get())
        print(m.get('foo'))
