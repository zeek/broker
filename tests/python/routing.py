import unittest
import time

import broker

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

    return ((ep1, ep2, ep3, ep4), (s1, s2, s3, s4))

def setup_peers_peering(opts1=None, opts2=None, opts3=None, opts4=None, create_s1=True, create_s2=True, create_s3=True, create_s4=True):
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
    def test_routing(self):
        ((ep1, ep2, ep3, ep4), (s1, s2, s3, s4)) = setup_peers_peering()

        s1bla = ep1.make_subscriber("/bla/")

        time.sleep(2)
        ep4.publish("/bla/", "Bla Bla Bla Bla Bla")

        x = s1bla.get()
        self.assertEqual(x, ("/bla", "Bla Bla Bla Bla Bla"))

    def test_routing_subscribe(self):
        ((ep1, ep2, ep3, ep4), (s1, s2, s3, s4)) = setup_peers()

        s1bla = ep1.make_subscriber("/bla/")
        time.sleep(1)
        # Peering
        p2 = ep2.listen("127.0.0.1", 0)
        p3 = ep3.listen("127.0.0.1", 0)
        p4 = ep4.listen("127.0.0.1", 0)
        # ep1<->ep2<->ep3<->ep4<->ep5
        ep1.peer("127.0.0.1", p2, 1.0)
        ep2.peer("127.0.0.1", p3, 1.0)
        ep3.peer("127.0.0.1", p4, 1.0)

        time.sleep(2)
        ep4.publish("/bla/", "Bla Bla Bla Bla Bla")

        x = s1bla.get()
        self.assertEqual(x, ("/bla", "Bla Bla Bla Bla Bla"))

    def test_routing_tree_cluster(self):
        ((ep1, ep2, ep3, ep4), (s1, s2, s3, s4)) = setup_peers()
        op = broker.BrokerOptions()
        op.forward=False
        ep5 = broker.Endpoint(broker.Configuration(op))

        # Peering
        p2 = ep2.listen("127.0.0.1", 0)
        p3 = ep3.listen("127.0.0.1", 0)
        p4 = ep4.listen("127.0.0.1", 0)
        p5 = ep5.listen("127.0.0.1", 0)
        #       ep1
        #      /   \
        #   ep2    ep3
        #    |
        #   ep4
        # + Proxy p5 connected to all peers, but not routable
        ep1.peer("127.0.0.1", p2, 1.0)
        ep2.peer("127.0.0.1", p4, 1.0)
        ep1.peer("127.0.0.1", p3, 1.0)

        ep1.peer("127.0.0.1", p5, 1.0)
        ep2.peer("127.0.0.1", p5, 1.0)
        ep3.peer("127.0.0.1", p5, 1.0)
        ep4.peer("127.0.0.1", p5, 1.0)

        time.sleep(1)

        s4bla = ep4.make_subscriber("/bla/")
        s3bla = ep3.make_subscriber("/bla/")

        time.sleep(1)
        ep1.publish("/bla/", "Bla Bla Bla Bla Bla")
        x = s4bla.get()
        self.assertEqual(x, ("/bla", "Bla Bla Bla Bla Bla"))
        x = s3bla.get()
        self.assertEqual(x, ("/bla", "Bla Bla Bla Bla Bla"))

if __name__ == '__main__':
   unittest.main(verbosity=3)
