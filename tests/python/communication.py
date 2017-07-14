
import unittest
import multiprocessing
import sys
import time

import broker

class TestCommunication(unittest.TestCase):
    def test_ping(self):
        ep1 = broker.Endpoint()
        ep2 = broker.Endpoint()
        es1 = ep1.make_subscriber("/test")
        es2 = ep2.make_subscriber("/test")
        port = ep1.listen("127.0.0.1", 0)
        ep2.peer("127.0.0.1", port, 1.0)

        ep2.publish("/test", ["ping"])
        (t, d) = es1.get()
        assert t == "/test"
        assert d[0] == "ping"

        ep1.publish(t, ["pong"])
        (t, d) = es2.get()
        assert t == "/test"
        assert d[0] == "pong"

        # TODO: This is needed so that the process terminates.
        # Need to find something better.
        es1 = es2 = ep1 = ep2 = None

    def test_messages(self):
        ep1 = broker.Endpoint()
        ep2 = broker.Endpoint()
        es1 = ep1.make_subscriber("/test")
        es2 = ep2.make_subscriber("/test")
        port = ep1.listen("127.0.0.1", 0)
        ep2.peer("127.0.0.1", port, 1.0)

        msg0 = ("/test/1", [])
        msg1 = ("/test/2", [1, 2, 3])
        msg2 = ("/test/3", [42, "foo", {"a": "A", "b": broker.Port(1947, broker.Port.Protocol.TCP)}])

        ep2.publish(*msg0)
        ep2.publish(*msg1)
        ep2.publish(*msg2)

        msgs = es1.get(3)
        assert not es1.available()

        assert msgs[0] == msg0
        assert msgs[1] == msg1
        assert msgs[2] == msg2

        # TODO: This is needed so that the process terminates.
        # Need to find something better.
        es1 = es2 = ep1 = ep2 = None

if __name__ == '__main__':
    # TestCommunication().test_messages()
    unittest.main(verbosity=3)
