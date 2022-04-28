
import unittest
import multiprocessing
import sys
import time
import ipaddress

import broker

class TestCommunication(unittest.TestCase):
    def test_ping(self):
        # --peer-start
        with broker.Endpoint() as ep1, \
             broker.Endpoint() as ep2, \
             ep1.make_subscriber("/test") as s1, \
             ep2.make_subscriber("/test") as s2:
            port = ep1.listen("127.0.0.1", 0)
            self.assertTrue(ep2.peer("127.0.0.1", port, 1.0))

            ep1.await_peer(ep2.node_id())
            ep2.await_peer(ep1.node_id())

            # --peer-end

            # --ping-start
            ep2.publish("/test", ["ping"])
            (t, d) = s1.get()
            # t == "/test", d == ["ping"]
            # --ping-end
            self.assertEqual(t, "/test")
            self.assertEqual(d[0], "ping")

            ep1.publish(t, ["pong"])

            while True:
                # This loop exists just for sake of test coverage for "poll()"
                msgs = s2.poll()

                if msgs:
                    self.assertEqual(len(msgs), 1)
                    (t, d) = msgs[0]
                    break;

                time.sleep(0.1)

            self.assertEqual(t, "/test")
            self.assertEqual(d[0], "pong")

    def test_messages(self):
        with broker.Endpoint() as ep1, \
             broker.Endpoint() as ep2, \
             ep1.make_subscriber("/test") as s1:

            port = ep1.listen("127.0.0.1", 0)
            self.assertTrue(ep2.peer("127.0.0.1", port, 1.0))

            ep1.await_peer(ep2.node_id())
            ep2.await_peer(ep1.node_id())

            msg0 = ("/test/1", ())
            ep2.publish(*msg0)

            # --messages-start
            msg1 = ("/test/2", (1, 2, 3))
            msg2 = ("/test/3", (42, "foo", {"a": "A", "b": ipaddress.IPv4Address('1.2.3.4')}))
            ep2.publish_batch(msg1, msg2)
            # --messages-end

            msgs = s1.get(3)
            self.assertFalse(s1.available())

            self.assertEqual(msgs[0], msg0)
            self.assertEqual(msgs[1], msg1)
            self.assertEqual(msgs[2], msg2)

            # These results are not (all) immutable: try modifying the third
            # value (the dict) of the last message above.
            dict_data = msgs[2][1][2]
            self.assertEqual(len(dict_data), 2)
            dict_data["c"] = "not immutable"
            self.assertEqual(len(dict_data), 3)

    def test_immutable_messages(self):
        with broker.Endpoint() as ep1, \
             broker.Endpoint() as ep2, \
             ep1.make_safe_subscriber("/test") as s1:

            port = ep1.listen("127.0.0.1", 0)
            ep2.peer("127.0.0.1", port, 1.0)

            msg = ("/test/1", ({"a": "A"}, set([1,2,3]), ('a', 'b', 'c')))
            ep2.publish(*msg)

            topic, (dict_data, set_data, tuple_data) = s1.get()

            # The return values are immutable, so each of the following triggers
            # a type-specific exception.
            with self.assertRaises(TypeError):
                # 'mappingproxy' object does not support item assignment
                dict_data["b"] = "B"
            with self.assertRaises(AttributeError):
                # 'frozenset' object has no attribute 'add'
                set_data.add(4)
            with self.assertRaises(TypeError):
                # 'tuple' object does not support item assignment
                tuple_data[3] = 'd'

    def test_publisher(self):
        with broker.Endpoint() as ep1, \
             broker.Endpoint() as ep2, \
             ep1.make_subscriber("/test") as s1, \
             ep2.make_publisher("/test") as p2:

            port = ep1.listen("127.0.0.1", 0)
            self.assertTrue(ep2.peer("127.0.0.1", port, 1.0))

            ep1.await_peer(ep2.node_id())
            ep2.await_peer(ep1.node_id())

            p2.publish([1, 2, 3])
            p2.publish_batch(["a", "b", "c"], [True, False])

            msgs = s1.get(3)
            self.assertFalse(s1.available())

            self.assertEqual(msgs[0], ("/test", (1, 2, 3)))
            self.assertEqual(msgs[1], ("/test", ("a", "b", "c")))
            self.assertEqual(msgs[2], ("/test", (True, False)))

    def test_status_subscriber(self):
        # --status-start
        with broker.Endpoint() as ep1, \
             broker.Endpoint() as ep2, \
             ep1.make_status_subscriber(True) as es1, \
             ep2.make_status_subscriber(True) as es2:

            port = ep1.listen("127.0.0.1", 0)
            self.assertEqual(ep2.peer("127.0.0.1", port, 1.0), True)

            ep1.await_peer(ep2.node_id())
            ep2.await_peer(ep1.node_id())

            st1 = es1.get(2)
            st2 = es2.get(2)
            # st1.code() == [broker.SC.EndpointDiscovered, broker.SC.PeerAdded]
            # st2.code() == [broker.SC.EndpointDiscovered, broker.SC.PeerAdded]
            # --status-end

            self.assertEqual(len(st1), 2)
            self.assertEqual(st1[0].code(), broker.SC.EndpointDiscovered)
            self.assertEqual(st1[1].code(), broker.SC.PeerAdded)
            self.assertEqual(len(st2), 2)
            self.assertEqual(st2[0].code(), broker.SC.EndpointDiscovered)
            self.assertEqual(st2[1].code(), broker.SC.PeerAdded)
            self.assertEqual(st2[1].context().network.get().address, "127.0.0.1")

    def test_status_subscriber_error(self):
        # --error-start
        with broker.Endpoint() as ep1, \
             ep1.make_status_subscriber() as es1:
            r = ep1.peer("127.0.0.1", 1947, 0.0) # Try unavailable port, no retry
            self.assertEqual(r, False) # Not shown in docs.
            st1 = es1.get()
            # s1.code() == broker.EC.PeerUnavailable
            # --error-end
            self.assertEqual(st1.code(), broker.EC.PeerUnavailable)

            # Async version.
            ep1.peer_nosync("127.0.0.1", 1947, 1.0)
            st1 = es1.get()
            self.assertEqual(st1.code(), broker.EC.PeerUnavailable)

            st1 = es1.get()
            self.assertEqual(st1.code(), broker.EC.PeerUnavailable)

    def test_idle_endpoint(self):
        with broker.Endpoint() as ep1, \
             ep1.make_status_subscriber() as es1, \
             ep1.make_subscriber("/test") as s1:

            pass

if __name__ == '__main__':
    unittest.main(verbosity=3)
