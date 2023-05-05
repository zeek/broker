from __future__ import print_function
import unittest
import multiprocessing

import broker

from zeek_common import run_zeek_path, run_zeek
from datetime import datetime

ZeekPing = """
redef Broker::default_connect_retry=1secs;
redef Broker::default_listen_retry=1secs;
redef exit_only_after_terminate = T;

global event_count: int = 0;

global ping: event(s: string, c: int);

event zeek_init()
    {
    Broker::subscribe("/test");
    Broker::peer("127.0.0.1", __PORT__/tcp);
    }

function send_event(s: string)
    {
    s += "x";

    if ( event_count == 5 )
        s += "\\x82";

    local e = Broker::make_event(ping, s, event_count);
    Broker::publish("/test", e);
    ++event_count;
    }

event Broker::peer_added(endpoint: Broker::EndpointInfo, s: string)
    {
    send_event("");
    }

event Broker::peer_lost(endpoint: Broker::EndpointInfo, msg: string)
    {
    terminate();
    }

event pong(s: string, n: int)
    {
    local ts = time_to_double(current_event_time());
    if ( ts == 23.0 * n )
        send_event(s);
    else if ( ts == 0.0 && s == "done" )
        send_event(s);
    else
        send_event(fmt("Unexpected timestamp: %s", ts));
    }
"""

class TestCommunication(unittest.TestCase):
    def test_ping(self):
        with broker.Endpoint() as ep, \
             ep.make_subscriber("/test") as sub:

            port = ep.listen("127.0.0.1", 0)

            p = multiprocessing.Process(target=run_zeek, args=(ZeekPing, port))
            p.daemon = True
            p.start()

            for i in range(0, 6):
                (t, msg) = sub.get()
                ev = broker.zeek.Event(msg)
                (s, c) = ev.args()
                expected_arg = "x" + "Xx" * i

                if i == 5:
                    expected_arg = expected_arg.encode('utf-8') + b'\x82'

                # Extract metadata.
                ev_metadata = ev.metadata()
                self.assertIsNotNone(ev_metadata)
                ev_metadata = dict(ev_metadata)
                self.assertIn(broker.zeek.MetadataType.NetworkTimestamp, ev_metadata)
                ts_ev = ev_metadata[broker.zeek.MetadataType.NetworkTimestamp]

                self.assertEqual(ev.name(), "ping")
                self.assertEqual(s, expected_arg)
                self.assertEqual(c, i)
                # Zeek's event timestamp should be before current time.
                ts_now = datetime.now(broker.utc)
                self.assertLess(ts_ev, ts_now)

                dt = datetime.fromtimestamp(23.0 * c, broker.utc)
                metadata = [(broker.zeek.MetadataType.NetworkTimestamp, dt)]

                if i < 3:
                    ev = broker.zeek.Event("pong", s + "X", c, metadata=metadata)
                elif i < 5:
                    ev = broker.zeek.Event("pong", s.encode('utf-8') + b'X', c, metadata=metadata)
                else:
                    ev = broker.zeek.Event("pong", 'done', c)

                ep.publish("/test", ev)

if __name__ == '__main__':
    unittest.main(verbosity=3)
