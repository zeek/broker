import unittest

from broker import *

class TestCommunication(unittest.TestCase):
  def test_endpoint_spawn(self):
    ctx = Context()
    ctx.spawn(Blocking)
    ctx.spawn(Nonblocking)
    with self.assertRaises(TypeError):
      ctx.spawn("yikes")

  def test_endpoint_blocking_local_subscription(self):
    ctx = Context()
    ep = ctx.spawn(Blocking)
    ep.subscribe("foo")
    ep.publish("foo.bar", 42)
    ep.publish("foo", [1, 2, 3])
    msg = ep.receive()
    self.assertEqual(msg.topic(), "foo.bar")
    # FIXME: type issue
    #self.assertEqual(msg.data(), 42)
    # FIXME: this statement just hangs.
    #ep.receive(lambda t, d: print("topic: %s, data: %s" % (t, d)))

if __name__ == '__main__':
  unittest.main(verbosity=3)
