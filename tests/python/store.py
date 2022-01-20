
import unittest
import sys
import time

import broker

from inspect import currentframe, getframeinfo

def create_stores(self):
    ep0 = broker.Endpoint()
    ep1 = broker.Endpoint()
    ep2 = broker.Endpoint()

    with ep0.make_subscriber("/test") as s0, \
         ep1.make_subscriber("/test") as s1, \
         ep1.make_status_subscriber() as es1, \
         ep2.make_subscriber("/test") as s2, \
         ep2.make_status_subscriber() as es2:

        p = ep0.listen("127.0.0.1", 0)
        ep1.peer("127.0.0.1", p)
        ep2.peer("127.0.0.1", p)

        # TODO: This doesn't work. Once it does, remove the event handshake.
        # es1.get()
        # es2.get()
        ep0.publish("/test", "go-ahead")
        s1.get()
        s2.get()
        ####

        m = ep0.attach_master("test", broker.Backend.Memory)
        c1 = ep1.attach_clone("test")
        c2 = ep2.attach_clone("test")

        return (ep0, ep1, ep2, m, c1, c2)

# Runs a test with one master and two clones
# --tri-setup-start
def run_tri_setup(self, f):
    with broker.Endpoint() as ep0, \
         broker.Endpoint() as ep1, \
         broker.Endpoint() as ep2, \
         ep0.attach_master("test", broker.Backend.Memory) as m, \
         ep1.attach_clone("test") as c1, \
         ep2.attach_clone("test") as c2:

        # connect the nodes
        port = ep0.listen("127.0.0.1", 0)
        self.assertTrue(ep1.peer("127.0.0.1", port))
        self.assertTrue(ep2.peer("127.0.0.1", port))

        # wait until the nodes are fully connected
        self.assertTrue(ep0.await_peer(ep1.node_id()))
        self.assertTrue(ep0.await_peer(ep2.node_id()))
        self.assertTrue(ep1.await_peer(ep0.node_id()))
        self.assertTrue(ep2.await_peer(ep0.node_id()))

        # wait until the clones have connected to the master
        self.assertTrue(c1.await_idle())
        self.assertTrue(c2.await_idle())

        f(m, c1, c2)
# --tri-setup-end

def await_idle(self, *argv):
    for store in argv:
        self.assertTrue(store.await_idle());

class TestStore(unittest.TestCase):
    def test_basic(self):
        # --master-start
        with broker.Endpoint() as ep1, \
             ep1.attach_master("test", broker.Backend.Memory) as m:

            m.put("key", "value")
            x = m.get("key")
            # x == "value"
            # --master-end
            self.assertEqual(x, "value")
            self.assertEqual(m.name(), "test")

    def test_from_master(self):
        def impl(m, c1, c2):
            v1 = "A"
            v2 = {"A", "B", "C"}
            v3 = {1: "A", 2: "B", 3: "C"}
            v4 = ("A", "B", "C")

            m.put("a", v1)
            m.put("b", v2)
            m.put("c", v3)
            m.put("d", v4)
            self.assertEqual(c2.put_unique("e", "first"), True)
            self.assertEqual(c2.put_unique("e", "second"), False)
            self.assertEqual(c2.put_unique("e", "third"), False)
            time.sleep(.5)

            def checkAccessors(x):
                self.assertEqual(x.get("a"), v1)
                self.assertEqual(x.get("b"), v2)
                self.assertEqual(x.get("c"), v3)
                self.assertEqual(x.get("d"), v4)
                self.assertEqual(x.get("e"), "first")
                self.assertEqual(x.get("X"), None)
                self.assertEqual(x.exists("d"), True)
                self.assertEqual(x.exists("X"), False)
                self.assertEqual(x.get_index_from_value("b", "A"), True)
                self.assertEqual(x.get_index_from_value("b", "X"), False)
                self.assertEqual(x.get_index_from_value("c", 1), "A")
                self.assertEqual(x.get_index_from_value("c", 10), None)
                self.assertEqual(x.get_index_from_value("d", 1), "B")
                self.assertEqual(x.get_index_from_value("d", 10), None)
                self.assertEqual(x.keys(), {'a', 'b', 'c', 'd', 'e'})

            checkAccessors(m)
            checkAccessors(c1)
            checkAccessors(c2)

            v5 = 5
            m.put("e", v5)
            m.put("f", v5)
            m.put("g", v5, 0.1)
            m.put("h", v5, 2)
            m.put("str", "b")
            m.put("vec", (1, 2))
            m.put("set", set([1, 2]))
            m.put("table", {1: "A", "2": "C"})

            # --ops-start
            m.increment("e", 1)
            m.decrement("f", 1)
            m.append("str", "ar")
            m.insert_into("set", 3)
            m.remove_from("set", 1)
            m.insert_into("table", 3, "D")
            m.remove_from("table", 1)
            m.push("vec", 3)
            m.push("vec", 4)
            m.pop("vec")
            # --ops-end

            time.sleep(0.15) # Make sure 'g' expires.

            await_idle(self, c2, c1, m)

            def checkModifiers(x):
                self.assertEqual(x.get("e"), v5 + 1)
                self.assertEqual(x.get("f"), v5 - 1)
                self.assertEqual(x.get("g"), None) # Expired
                self.assertEqual(x.get("h"), v5) # Not Expired
                self.assertEqual(x.get("str"), "bar")
                self.assertEqual(x.get("set"), set([2, 3]))
                self.assertEqual(x.get("table"), {3: "D", "2": "C"})
                self.assertEqual(x.get("vec"), (1, 2, 3))

            checkModifiers(m)
            checkModifiers(c1)
            checkModifiers(c2)

            m.clear()

            await_idle(self, c2, c1, m)

            self.assertEqual(m.keys(), set())
            self.assertEqual(c1.keys(), set())
            self.assertEqual(c2.keys(), set())

        run_tri_setup(self, impl)

    def test_from_one_clone(self):
        with broker.Endpoint() as ep0, \
             broker.Endpoint() as ep1, \
             ep0.attach_master("test", broker.Backend.Memory) as m, \
             ep1.attach_clone("test") as c1:

            port = ep0.listen("127.0.0.1", 0)

            self.assertTrue(ep1.peer("127.0.0.1", port))

            ep0.await_peer(ep1.node_id())
            ep1.await_peer(ep0.node_id())

            v1 = "A"
            v2 = {"A", "B", "C"}
            v3 = {1: "A", 2: "B", 3: "C"}
            v4 = ("A", "B", "C")

            c1.put("a", v1)
            c1.put("b", v2)

            await_idle(self, c1, m)

            self.assertEqual(c1.put_unique("e", "first"), True)


    def test_from_two_clones(self):
        def impl(m, c1, c2):

            v1 = "A"
            v2 = {"A", "B", "C"}
            v3 = {1: "A", 2: "B", 3: "C"}
            v4 = ("A", "B", "C")

            c1.put("a", v1)
            c1.put("b", v2)
            c2.put("c", v3)
            c2.put("d", v4)
            self.assertEqual(c2.put_unique("e", "first"), True)
            self.assertEqual(c2.put_unique("e", "second"), False)
            self.assertEqual(c2.put_unique("e", "third"), False)

            await_idle(self, c1, c2)

            def checkAccessors(x):
                self.assertEqual(x.get("a"), v1)
                self.assertEqual(x.get("b"), v2)
                self.assertEqual(x.get("c"), v3)
                self.assertEqual(x.get("d"), v4)
                self.assertEqual(x.get("e"), "first")
                self.assertEqual(x.get("X"), None)
                self.assertEqual(x.exists("d"), True)
                self.assertEqual(x.exists("X"), False)
                self.assertEqual(x.get_index_from_value("b", "A"), True)
                self.assertEqual(x.get_index_from_value("b", "X"), False)
                self.assertEqual(x.get_index_from_value("c", 1), "A")
                self.assertEqual(x.get_index_from_value("c", 10), None)
                self.assertEqual(x.get_index_from_value("d", 1), "B")
                self.assertEqual(x.get_index_from_value("d", 10), None)
                self.assertEqual(x.keys(), {'a', 'b', 'c', 'd', 'e'})

            checkAccessors(m)
            checkAccessors(c1)
            checkAccessors(c2)

            v5 = 5
            c1.put("e", v5)
            c2.put("f", v5)
            c1.put("g", v5, 0.1)
            c2.put("h", v5, 20)
            m.put("str", "b")
            m.put("vec", [1, 2])
            m.put("set", set([1, 2]))
            m.put("table", {1: "A", "2": "C"})

            await_idle(self, c1, c2, m)

            c2.increment("e", 1)
            c1.decrement("f", 1)
            c2.append("str", "ar")
            c1.insert_into("set", 3)
            c2.remove_from("set", 1)
            c1.insert_into("table", 3, "D")
            c2.remove_from("table", 1)
            c1.push("vec", 3)

            await_idle(self, c1, c2, m)

            c2.push("vec", 4)
            c2.pop("vec")

            await_idle(self, c1, c2, m)

            def checkModifiers(x):
                self.assertEqual(x.get("e"), v5 + 1)
                self.assertEqual(x.get("f"), v5 - 1)
                self.assertEqual(x.get("g"), None) # Expired
                self.assertEqual(x.get("h"), v5) # Not Expired
                self.assertEqual(x.get("str"), "bar")
                self.assertEqual(x.get("set"), set([2, 3]))
                self.assertEqual(x.get("table"), {3: "D", "2": "C"})
                self.assertEqual(x.get("vec"), (1, 2, 3))

            checkModifiers(m)
            checkModifiers(c1)
            checkModifiers(c2)

            m.clear()

            await_idle(self, c1, c2, m)

            self.assertEqual(m.keys(), set())
            self.assertEqual(c1.keys(), set())
            self.assertEqual(c2.keys(), set())

        run_tri_setup(self, impl)

if __name__ == '__main__':
    unittest.main(verbosity=3)
