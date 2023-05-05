"""
Test the broker.zeek module without involving Zeek because speed.
"""
import datetime
import unittest

import broker
import broker.zeek

NetworkTimestamp = broker.zeek.MetadataType.NetworkTimestamp

class TestEventMetadata(unittest.TestCase):

    dt = datetime.datetime(2023, 5, 3, 9, 27, 57, tzinfo=broker.utc)

    def test_event_no_metadata(self):
        e = broker.zeek.Event("a", [42, "a"])
        self.assertTrue(e.valid())
        self.assertIsNone(e.metadata())

    def test_event_metadata_network_timestamp(self):
        e = broker.zeek.Event("a", [42, "a"], metadata=[(NetworkTimestamp, self.dt)])
        self.assertTrue(e.valid())
        metadata = e.metadata()
        self.assertEqual(len(metadata), 1)
        key, value = metadata[0]
        self.assertEqual(key, NetworkTimestamp)
        self.assertEqual(value, self.dt)

    def test_event_metadata_dict_integer_convenience(self):
        e = broker.zeek.Event("a", [42, "a"], metadata={1: self.dt})
        self.assertTrue(e.valid())
        self.assertEqual(len(e.metadata()), 1)

    def test_event_metadata_invalid_network_timestamp_1(self):
        e = broker.zeek.Event("a", [42, "a"], metadata=[(NetworkTimestamp, "invalid")])
        self.assertFalse(e.valid())

    def test_event_metadata_bad_timestamp_2(self):
        e = broker.zeek.Event("a", [42, "a"], metadata=[(NetworkTimestamp, 1.234)])
        self.assertFalse(e.valid())

class TestCommunication(unittest.TestCase):

    dt = datetime.datetime(2023, 5, 3, 9, 27, 57, tzinfo=broker.utc)

    def test_event_metadata(self):
        with broker.Endpoint() as ep1, \
             broker.Endpoint() as ep2, \
             ep1.make_subscriber("/test") as s1:

            port = ep1.listen("127.0.0.1", 0)
            self.assertTrue(ep2.peer("127.0.0.1", port, 1.0))

            ep1.await_peer(ep2.node_id())
            ep2.await_peer(ep1.node_id())

            metadata = {
                NetworkTimestamp: self.dt,
                broker.Count(1234): "custom",
            }

            ev = broker.zeek.Event("ping", "X", metadata=metadata)
            ep2.publish("/test", ev)
            (t, d) = s1.get()
            recv_ev = broker.zeek.Event(d)
            self.assertTrue(recv_ev.valid())

            metadata = recv_ev.metadata()
            self.assertEqual(len(metadata), 2)
            metadata_dict = dict(metadata)
            self.assertEqual(metadata_dict[NetworkTimestamp], self.dt)
            self.assertEqual(metadata_dict[broker.Count(1234)], "custom")


if __name__ == '__main__':
    unittest.main(verbosity=3)
