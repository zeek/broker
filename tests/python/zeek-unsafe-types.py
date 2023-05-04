from __future__ import print_function
import unittest
import multiprocessing

import broker

from zeek_common import run_zeek_path, run_zeek

ZeekHello = """
redef Broker::default_connect_retry=1secs;
redef Broker::default_listen_retry=1secs;
redef exit_only_after_terminate = T;

type Container: record {
    config: table[string] of string &default=table();
};

type Containers: record {
    containers: set[Container] &default=set();
};

global hello: event(cs: Containers);

event zeek_init()
    {
    Broker::subscribe("/test");
    Broker::peer("127.0.0.1", __PORT__/tcp);
    }

event Broker::peer_added(endpoint: Broker::EndpointInfo, s: string)
    {
    local container = Container($config = table(["foo"] = "bar"));
    local containers = Containers($containers=set(container));

    Broker::publish("/test", hello, containers);
    }

event Broker::peer_lost(endpoint: Broker::EndpointInfo, msg: string)
    {
    terminate();
    }
"""

class TestCommunication(unittest.TestCase):
    def test_regular(self):
        with broker.Endpoint() as ep, \
             ep.make_subscriber("/test") as sub:

            port = ep.listen("127.0.0.1", 0)

            p = multiprocessing.Process(target=run_zeek, args=(ZeekHello, port))
            p.daemon = True
            p.start()

            # With a regular subscriber, retrieving the hello event
            # will lead to a TypeError since it contains a set of tables:
            with self.assertRaises(TypeError) as ctx:
                t, msg = sub.get()

            self.assertEqual(str(ctx.exception), "unhashable type: 'dict'")

    def test_safe(self):
        with broker.Endpoint() as ep, \
             ep.make_safe_subscriber("/test") as sub:

            port = ep.listen("127.0.0.1", 0)

            p = multiprocessing.Process(target=run_zeek, args=(ZeekHello, port))
            p.daemon = True
            p.start()

            # With a SafeSubscriber, the retrieval works because we now use
            # ImmutableData under the hood:
            t, msg = sub.get()

            # The broker.zeek.Event class has a similar problem, since it uses
            # broker.Data to render Broker data:
            with self.assertRaises(TypeError) as ctx:
                ev = broker.zeek.Event(msg)
                args = ev.args()

            self.assertEqual(str(ctx.exception), "unhashable type: 'dict'")

            # broker.zeek.SafeEvent uses broker.ImmutableData, so can access
            # the arguments safely:
            ev = broker.zeek.SafeEvent(msg)
            args = ev.args()

if __name__ == '__main__':
    unittest.main(verbosity=3)
