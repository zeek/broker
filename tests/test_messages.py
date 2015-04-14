#!/usr/bin/env python

from pybroker import *
from select import select

epl = endpoint("listener")
mql = message_queue("mytopic", epl)
icsq = epl.incoming_connection_status()

epc = endpoint("connector")
ocsq = epc.outgoing_connection_status()

epl.listen(9999, "127.0.0.1")
epc.peer("127.0.0.1", 9999, 1)

select([icsq.fd()],[],[])
msgs = icsq.want_pop()

for m in msgs:
    print("incoming connection", m.peer_name, m.status)
    assert(m.peer_name == "connector")
    assert(m.status == incoming_connection_status.tag_established)

select([ocsq.fd()],[],[])
msgs = ocsq.want_pop()

for m in msgs:
    print("outgoing connection", m.peer_name, m.status)
    assert(m.peer_name == "listener")
    assert(m.status == outgoing_connection_status.tag_established)

v = vector_of_data([data(1), data(2), data("three")])
r = record(vector_of_field([field(data(1)), field(), field(data("hi")), field(data(2.2))]))
m = message([data("hi"), data("bye"), data(13), data(1.3), data(address.from_string("127.0.0.1"))])
m.push_back(data(v))
m.push_back(data(r))
epc.send("mytopic", m)

select([mql.fd()], [], [])
msgs = mql.want_pop()

for m in msgs:
    print("got message")
    i = 0

    for d in m:
        print(i, str(d))

        if i == 0:
            assert(d.which() == data.tag_string)
            assert(d.as_string() == "hi")
        elif i == 1:
            assert(d.which() == data.tag_string)
            assert(d.as_string() == "bye")
        elif i == 2:
            assert(d.which() == data.tag_count)
            assert(d.as_count() == 13)
        elif i == 3:
            assert(d.which() == data.tag_real)
            assert(d.as_real() == 1.3)
        elif i == 4:
            assert(d.which() == data.tag_address)
            assert(str(d.as_address()) == "127.0.0.1")
        elif i == 5:
            assert(d.which() == data.tag_vector)
            vec = d.as_vector()
            assert(vec[0] == v[0])
            assert(vec[1] == v[1])
            assert(vec[2] == v[2])
        elif i == 6:
            assert(d.which() == data.tag_record)
            rec = d.as_record()
            assert(rec == r)
            assert(rec.fields()[0].valid() == True)
            assert(rec.fields()[0].get().which() == data.tag_count)
            assert(rec.fields()[0].get().as_count() == 1)
            assert(rec.fields()[1].valid() == False)

        i += 1
