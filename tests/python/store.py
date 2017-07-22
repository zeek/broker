
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

    def test_from_master(self):
        (ep0, ep1, ep2, m, c1, c2) = create_stores()

        v1 = "A"
        v2 = {"A", "B", "C"}
        v3 = {1: "A", 2: "B", 3: "C"}
        v4 = ["A", "B", "C"]

        m.put("a", v1)
        m.put("b", v2)
        m.put("c", v3)
        m.put("d", v4)
        time.sleep(.5)

        def checkAccessors(x):
            self.assertEqual(x.get("a"), v1)
            self.assertEqual(x.get("b"), v2)
            self.assertEqual(x.get("c"), v3)
            self.assertEqual(x.get("d"), v4)
            self.assertEqual(x.get("X"), None)
            self.assertEqual(x.get("b", "A"), True)
            self.assertEqual(x.get("b", "X"), False)
            self.assertEqual(x.get("c", 1), "A")
            self.assertEqual(x.get("c", 10), None)
            self.assertEqual(x.get("d", 1), "B")
            self.assertEqual(x.get("d", 10), None)
            self.assertEqual(x.keys(), {'a', 'b', 'c', 'd'})

        checkAccessors(m)
        checkAccessors(c1)
        checkAccessors(c2)

        v5 = 5
        m.put("e", v5)
        m.put("f", v5)
        m.add("e", 1)
        m.put("g", v5, 0.1)
        m.put("h", v5, 2)
        m.subtract("f", 1)
        time.sleep(.5)

        def checkModifiers(x):
            self.assertEqual(x.get("e"), v5 + 1)
            self.assertEqual(x.get("f"), v5 - 1)
            self.assertEqual(x.get("g"), None) # Expired
            self.assertEqual(x.get("h"), v5) # Not Expired

        checkModifiers(m)
        checkModifiers(c1)
        checkModifiers(c2)

        m.clear()
        time.sleep(.5)
        self.assertEqual(m.keys(), set())
        self.assertEqual(c1.keys(), set())
        self.assertEqual(c2.keys(), set())

        m = c1 = c2 = None
        ep0 = ep1 = ep2

    def test_from_clones(self):
        (ep0, ep1, ep2, m, c1, c2) = create_stores()

        v1 = "A"
        v2 = {"A", "B", "C"}
        v3 = {1: "A", 2: "B", 3: "C"}
        v4 = ["A", "B", "C"]

        c1.put("a", v1)
        c1.put("b", v2)
        c2.put("c", v3)
        c2.put("d", v4)
        time.sleep(.5)

        def checkAccessors(x):
            self.assertEqual(x.get("a"), v1)
            self.assertEqual(x.get("b"), v2)
            self.assertEqual(x.get("c"), v3)
            self.assertEqual(x.get("d"), v4)
            self.assertEqual(x.get("X"), None)
            self.assertEqual(x.get("b", "A"), True)
            self.assertEqual(x.get("b", "X"), False)
            self.assertEqual(x.get("c", 1), "A")
            self.assertEqual(x.get("c", 10), None)
            self.assertEqual(x.get("d", 1), "B")
            self.assertEqual(x.get("d", 10), None)
            self.assertEqual(x.keys(), {'a', 'b', 'c', 'd'})

        checkAccessors(m)
        checkAccessors(c1)
        checkAccessors(c2)

        v5 = 5
        c1.put("e", v5)
        c2.put("f", v5)
        c1.put("g", v5, 0.1)
        c2.put("h", v5, 2)
        time.sleep(.5)
        c2.add("e", 1)
        c1.subtract("f", 1)
        time.sleep(.5)

        def checkModifiers(x):
            self.assertEqual(x.get("e"), v5 + 1)
            self.assertEqual(x.get("f"), v5 - 1)
            self.assertEqual(x.get("g"), None) # Expired
            self.assertEqual(x.get("h"), v5) # Not Expired

        checkModifiers(m)
        checkModifiers(c1)
        checkModifiers(c2)

        m.clear()
        time.sleep(.5)
        self.assertEqual(m.keys(), set())
        self.assertEqual(c1.keys(), set())
        self.assertEqual(c2.keys(), set())

        m = c1 = c2 = None
        ep0 = ep1 = ep2


if __name__ == '__main__':
    unittest.main(verbosity=3)
