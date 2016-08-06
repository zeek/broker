import unittest

from broker import *

class TestCommunication(unittest.TestCase):
  def test_endpoint_construction(self):
    ctx = Context()
    ep0 = ctx.spawn(Blocking)
    ep1 = ctx.spawn(Nonblocking)
    with self.assertRaises(BrokerError):
      ctx.spawn("yikes")

  def test_endpoint_blocking_local_subscription(self):
    ctx = Context()
    ep = ctx.spawn(Blocking)
    ep.subscribe("foo")
    ep.publish("foo.bar", 42)
    ep.publish("foo", [1, 2, 3])
    print(ep.receive())
    print(ep.receive())
    #ep.receive(lambda t, d: print("topic: %s, data: %s" % (t, d)))

if __name__ == '__main__':
  unittest.main(verbosity=3)
