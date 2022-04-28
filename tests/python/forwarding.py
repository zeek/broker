
import unittest
import multiprocessing
import sys
import time

import broker

def cleanup(es, ss):
    for s in ss:
        if s:
            s.reset()

    for e in es:
        e.shutdown()

def setup_peers(opts1=None, opts2=None, opts3=None, opts4=None, create_s1=True, create_s2=True, create_s3=True, create_s4=True):
    def cfg(opts):
        return broker.Configuration(opts) if opts else broker.Configuration(broker.BrokerOptions())
    ep1 = broker.Endpoint(cfg(opts1))
    ep2 = broker.Endpoint(cfg(opts2))
    ep3 = broker.Endpoint(cfg(opts3))
    ep4 = broker.Endpoint(cfg(opts4))

    s1 = ep1.make_subscriber("/test/") if create_s1 else None
    s2 = ep2.make_subscriber("/test/") if create_s2 else None
    s3 = ep3.make_subscriber("/test/") if create_s3 else None
    s4 = ep4.make_subscriber("/test/") if create_s4 else None

    p2 = ep2.listen("127.0.0.1", 0)
    p3 = ep3.listen("127.0.0.1", 0)
    p4 = ep4.listen("127.0.0.1", 0)

    # ep1 <-> ep2 <-> ep3 <-> ep4
    ep1.peer("127.0.0.1", p2, 1.0)
    ep2.peer("127.0.0.1", p3, 1.0)
    ep3.peer("127.0.0.1", p4, 1.0)

    return ((ep1, ep2, ep3, ep4), (s1, s2, s3, s4))

class TestCommunication(unittest.TestCase):
    def test_two_subscribed_hops(self):
        # Two hops that are subscribed.
        ((ep1, ep2, ep3, ep4), (s1, s2, s3, s4)) = setup_peers()

        ep1.await_peer(ep4.node_id())
        ep4.await_peer(ep1.node_id())

        ep1.publish("/test/foo", "Foo!")
        ep4.publish("/test/bar", "Bar!")

        x = s4.get()
        self.assertEqual(x, ('/test/foo', 'Foo!'))
        x = s1.get()
        self.assertEqual(x, ('/test/bar', 'Bar!'))
        cleanup((ep1, ep2, ep3, ep4), (s1, s2, s3, s4))

#### Note: disabled until we switch back to source-routing.
#
#    def test_two_unsubscribed_hops(self):
#        # Two hops that are not subscribed, but still forward due to the source
#        # routing.
#        ((ep1, ep2, ep3, ep4), (s1, s2, s3, s4)) = setup_peers(create_s2=False, create_s3=False)
#
#        ep1.await_peer(ep4.node_id())
#        ep4.await_peer(ep1.node_id())
#
#        ep1.publish("/test/foo", "Foo!")
#        ep4.publish("/test/bar", "Bar!")
#
#        x = s4.get()
#        self.assertEqual(x, ('/test/foo', 'Foo!'))
#        x = s1.get()
#        self.assertEqual(x, ('/test/bar', 'Bar!'))
#        cleanup((ep1, ep2, ep3, ep4), (s1, s2, s3, s4))

if __name__ == '__main__':
    #TestCommunication().test_two_hops()
    unittest.main(verbosity=3)
