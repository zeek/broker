
import unittest
import sys
import time

import broker

def create_stores():
    ep0 = broker.Endpoint()
    s0 = ep0.make_subscriber("/test")
    p = ep0.listen("127.0.0.1", 0)

    ep1 = broker.Endpoint()
    s1 = ep1.make_subscriber("/test")
    es1 = ep1.make_event_subscriber()
    ep1.peer("127.0.0.1", p)

    ep2 = broker.Endpoint()
    s2 = ep2.make_subscriber("/test")
    es2 = ep2.make_event_subscriber()
    ep2.peer("127.0.0.1", p)

    # TODO: This doesn't work. Once it does, remove the event handshake.
    # es1.get()
    # es2.get()
    ep0.publish("/test", "go-ahead")
    s1.get()
    s2.get()
    ####

    m = ep0.attach("test", broker.Backend.Memory)
    c1 = ep1.attach("test")
    c2 = ep2.attach("test")

    return (ep0, ep1, ep2, m, c1, c2)

class TestStore(unittest.TestCase):
    def test_basic(self):
        ep1 = broker.Endpoint()
        m = ep1.attach("test", broker.Backend.Memory)
        m.put("key", "value")
        x = m.get("key")
        self.assertEqual(x, "value")
        self.assertEqual(m.name(), "test")

        # TODO: This is so that the process shuts down.
        m = None
        ep1 = None

    def test_multi_clones(self):
        (ep0, ep1, ep2, m, c1, c2) = create_stores()

        v1 = "A"
        v2 = {"A", "B", "C"}
        v3 = {1: "A", 2: "B", 3: "C"}
        v4 = ["A", "B", "C"]

        m.put("a", v1)
        m.put("b", v2)
        m.put("c", v3)
        m.put("d", v4)

        def testOps(x):
            self.assertEqual(x.get("a"), v1)
            self.assertEqual(x.get("b"), v2)
            self.assertEqual(x.get("c"), v3)
            self.assertEqual(x.get("d"), v4)
            self.assertEqual(x.get("X"), None)

            # TODO: These fail for clones currently.
            self.assertEqual(x.get("b", "A"), True)
            self.assertEqual(x.get("b", "X"), False)
            self.assertEqual(x.get("c", 1), "A")
            self.assertEqual(x.get("c", 10), None)
            self.assertEqual(x.get("d", 1), "B")
            self.assertEqual(x.get("d", 10), None)

        testOps(m)
        # Note: There's a slight delay until the data gets into the clones, but
        # doing the master tests first seems good enough. At least I haven't
        # seen this break so far unless I take them out.
        testOps(c1)
        testOps(c2)

        m = c1 = c2 = None
        ep0 = ep1 = ep2

if __name__ == '__main__':
    TestStore().test_multi_clones()
    #unittest.main(verbosity=3)
