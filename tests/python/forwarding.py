
import unittest
import multiprocessing
import sys
import time

import broker

def setup_peers(opts1=None, opts2=None, opts3=None, opts4=None):
    def cfg(opts):
        return broker.Configuration(opts) if opts else broker.Configuration(broker.BrokerOptions())
    ep1 = broker.Endpoint(cfg(opts1))
    ep2 = broker.Endpoint(cfg(opts2))
    ep3 = broker.Endpoint(cfg(opts3))
    ep4 = broker.Endpoint(cfg(opts4))

    s1 = ep1.make_subscriber("/test/")
    s2 = ep2.make_subscriber("/test/") # (*)
    s3 = ep3.make_subscriber("/test/") # (*)
    s4 = ep4.make_subscriber("/test/")

    p2 = ep2.listen("127.0.0.1", 0)
    p3 = ep3.listen("127.0.0.1", 0)
    p4 = ep4.listen("127.0.0.1", 0)

    # ep1 <-> ep2 <-> ep3 <-> ep4
    ep1.peer("127.0.0.1", p2, 1.0)
    ep2.peer("127.0.0.1", p3, 1.0)
    ep3.peer("127.0.0.1", p4, 1.0)

    return ((ep1, ep2, ep3, ep4), (s1, s2, s3, s4))

class TestCommunication(unittest.TestCase):
    def test_two_hops(self):
        ((ep1, ep2, ep3, ep4), (s1, s2, s3, s4)) = setup_peers()

        ep1.publish("/test/foo", "Foo!")
        ep4.publish("/test/bar", "Bar!")

        x = s4.get()
        self.assertEqual(x, ('/test/foo', 'Foo!'))
        x = s1.get()
        self.assertEqual(x, ('/test/bar', 'Bar!'))

    def test_two_hops_no_forward(self):
        no_forward = broker.BrokerOptions()
        no_forward.forward = False

        ((ep1, ep2, ep3, ep4), (s1, s2, s3, s4)) = setup_peers(opts2=no_forward)

        ep1.publish("/test/foo", "Foo!") # Shouldn't arrive
        x = s4.get(1.0)
        self.assertEqual(x, None)

    def test_two_hops_ttl(self):
        # Note the 1st receiver's TTL value is the one that's applied.
        ttl2 = broker.BrokerOptions()
        ttl2.ttl = 2
        ((ep1, ep2, ep3, ep4), (s1, s2, s3, s4)) = setup_peers(opts2=ttl2)

        ep1.publish("/test/foo", "Foo!")

        x = s2.get(1.0)
        self.assertEqual(x, ('/test/foo', 'Foo!'))
        x = s3.get(1.0)
        self.assertEqual(x, ('/test/foo', 'Foo!'))
        x = s4.get(1.0)
        self.assertEqual(x, None) # Doesn't get here anymore.

if __name__ == '__main__':
    TestCommunication().test_two_hops_ttl()
    #unittest.main(verbosity=3)
