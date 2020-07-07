# sqlite-listen.py

import broker

ep = broker.Endpoint()
s = ep.make_subscriber('/test')
ss = ep.make_status_subscriber(True);
ep.listen('127.0.0.1', 9999)

m = ep.attach_master('mystore',
                     broker.Backend.SQLite, {'path': 'mystore.sqlite'})

while True:
    print(ss.get())
    print(m.get('foo'))
