# ping.py

import sys

import broker

# Setup endpoint and connect to Zeek.
with (
    broker.Endpoint() as ep,
    ep.make_subscriber("/topic/test") as sub,
    ep.make_status_subscriber(True) as ss,
):
    ep.peer("127.0.0.1", 9999)

    # Wait until connection is established.
    st = ss.get()

    if not (type(st) == broker.Status and st.code() == broker.SC.PeerAdded):
        print("could not connect")
        sys.exit(0)

    for n in range(5):
        # Send event "ping(n)".
        ping = broker.zeek.Event("ping", n)
        ep.publish("/topic/test", ping)
        # Wait for "pong" reply event.
        (t, d) = sub.get()
        pong = broker.zeek.Event(d)
        print(f"received {pong.name()}{pong.args()}")
