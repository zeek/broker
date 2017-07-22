\
import unittest
import multiprocessing
import sys
import time

import broker

class TestCommunication(unittest.TestCase):
    def test_ping(self):
        ep1 = broker.Endpoint()
        ep2 = broker.Endpoint()
        s1 = ep1.make_subscriber("/test")
        s2 = ep2.make_subscriber("/test")
        port = ep1.listen("127.0.0.1", 0)
        ep2.peer("127.0.0.1", port, 1.0)

        ep2.publish("/test", ["ping"])
        (t, d) = s1.get()
        self.assertEqual(t, "/test")
        self.assertEqual(d[0], "ping")

        ep1.publish(t, ["pong"])
        (t, d) = s2.get()
        self.assertEqual(t, "/test")
        self.assertEqual(d[0], "pong")

        # TODO: This is needed so that the process terminates.
        # Need to find something better.
        s1 = s2 = ep1 = ep2 = None

    def test_messages(self):
        ep1 = broker.Endpoint()
        ep2 = broker.Endpoint()
        s1 = ep1.make_subscriber("/test")
        port = ep1.listen("127.0.0.1", 0)
        ep2.peer("127.0.0.1", port, 1.0)

        msg0 = ("/test/1", [])
        msg1 = ("/test/2", [1, 2, 3])
        msg2 = ("/test/3", [42, "foo", {"a": "A", "b": broker.Port(1947, broker.Port.Protocol.TCP)}])

        ep2.publish(*msg0)
        ep2.publish_batch(msg1, msg2)

        msgs = s1.get(3)
        self.assertFalse(s1.available())

        self.assertEqual(msgs[0], msg0)
        self.assertEqual(msgs[1], msg1)
        self.assertEqual(msgs[2], msg2)

        # TODO: This is needed so that the process terminates.
        # Need to find something better.
        s1 = ep1 = ep2 = None

    def test_publisher(self):
        ep1 = broker.Endpoint()
        ep2 = broker.Endpoint()
        s1 = ep1.make_subscriber("/test")
        p2 = ep2.make_publisher("/test")
        port = ep1.listen("127.0.0.1", 0)
        ep2.peer("127.0.0.1", port, 1.0)

        p2.publish([1, 2, 3])
        p2.publish_batch(["a", "b", "c"], [True, False])

        msgs = s1.get(3)
        self.assertFalse(s1.available())

        self.assertEqual(msgs[0], ("/test", [1, 2, 3]))
        self.assertEqual(msgs[1], ("/test", ["a", "b", "c"]))
        self.assertEqual(msgs[2], ("/test", [True, False]))

        # TODO: This *still* doesn't terminate, even with this manual
        # cleanup here.
        p2 = s1 = None
        ep1 = ep2 = None

    def test_event_subscriber(self):
        ep1 = broker.Endpoint()
        ep2 = broker.Endpoint()
        es1 = ep1.make_event_subscriber(True)
        es2 = ep2.make_event_subscriber(True)
        port = ep1.listen("127.0.0.1", 0)
        ep2.peer("127.0.0.1", port, 1.0)

        st1 = es1.get()
        st2 = es2.get()

        self.assertEqual(st1.code(), broker.SC.PeerAdded)
        self.assertEqual(st1.context().network.get().address, "127.0.0.1")
        self.assertEqual(st2.code(), broker.SC.PeerAdded)
        self.assertEqual(st2.context().network.get().address, "127.0.0.1")

        # TODO: This is needed so that the process terminates.
        # Need to find something better. Note that even the order is
        # important.
        es1 = es2 = None
        s1 = s2 = ep1 = ep2 = None

    def test_event_subscriber_error(self):
        ep1 = broker.Endpoint()
        es1 = ep1.make_event_subscriber()

        r = ep1.peer("127.0.0.1", 1947, 0.0)
        self.assertEqual(r, False)

        # TODO: This never returns. Bug?
        st1 = es1.get()
        self.assertEqual(st1, broker.EC.PeerUnavailable)

        # TODO: This is needed so that the process terminates.
        # Need to find something better. Note that even the order is
        # important.
        es1 = None
        ep1 = None

if __name__ == '__main__':
    unittest.main(verbosity=3)
