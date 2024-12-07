# ping.py

import sys
import time

import broker

event = int(sys.argv[1])

total_sent_ev1 = 0
total_recv_ev1 = 0
first_t = float(time.time())

last_t = first_t
last_sent_ev1 = 0


def printStats(stats):
    t = stats[0]
    dt = stats[1]
    ev1 = stats[1 + event].value

    global total_recv_ev1
    total_recv_ev1 += ev1

    global last_t, last_sent_ev1
    now = time.time()
    # rate = "sending at {:.2f} ev/s, receiving at {:.2f} ev/s".format(total_sent_ev1 / (now - first_t) , total_recv_ev1 / (now - first_t))
    rate = f"sending at {(total_sent_ev1 - last_sent_ev1) / (now - last_t):.2f} ev/s, receiving at {ev1 / dt.total_seconds():.2f} ev/s"
    last_t = now
    last_sent_ev1 = total_sent_ev1

    print(
        f"{t} dt={dt} ev{event}={ev1} (total {total_recv_ev1} of {total_sent_ev1}) {rate}"
    )


def sendBatch(p, num):
    event_1s = [broker.zeek.Event(f"event_{event}", [i, "test"]) for i in range(num)]
    for e in event_1s:
        p.publish(e)

    global total_sent_ev1
    total_sent_ev1 += len(event_1s)


def wait(s, t):
    waited = 0

    while True:
        msgs = s.poll()

        for m in msgs:
            e = broker.zeek.Event(m[1])
            if e.name() == "stats_update":
                printStats(e.args()[0])

        time.sleep(0.01)
        waited += 0.01
        if waited >= t:
            break


ep = broker.Endpoint()
s = ep.make_subscriber("/benchmark/stats")
ss = ep.make_status_subscriber(True)
ep.peer("127.0.0.1", 9999)

# Wait until connection is established.
st = ss.get()

if not (type(st) == broker.Status and st.code() == broker.SC.PeerAdded):
    print("could not connect")
    sys.exit(1)

p = ep.make_publisher("/benchmark/events")

while True:
    sendBatch(p, 5000)
    wait(s, 0.001)

    if ss.available():
        print(ss.get())
        sys.exit(0)
