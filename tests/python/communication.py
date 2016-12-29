import unittest

from broker import *

class TestCommunication(unittest.TestCase):
  def test_endpoint_spawn(self):
    ctx = Context()
    ctx.spawn(Blocking)
    ctx.spawn(Nonblocking)
    with self.assertRaises(TypeError):
      ctx.spawn("yikes")

  def test_endpoint_subscription_forwarding(self):
    ctx = Context()
    x = ctx.spawn(Blocking)
    y = ctx.spawn(Blocking)
    x.peer(y)
    s = x.receive(Status)
    print(s)
    #x.publish("foo", 4) # nobody subscribed yet
    #x.subscribe("foo")
    #x.publish("foo", 2) # forward to peer
    #x.unsubscribe("foo")
    #x.publish("foo", 0) # no more

  def test_endpoint_blocking_local_subscription(self):
    ctx = Context()
    ep = ctx.spawn(Blocking)
    ep.subscribe("foo")
    ep.publish("foo.bar", 42)
    ep.publish("foo", [1, 2, 3])
    msg = ep.receive(Message)
    self.assertEqual(msg.topic(), "foo.bar")
    # FIXME: data needs to be unwrapped
    #self.assertEqual(msg.data(), 42)

if __name__ == '__main__':
  unittest.main(verbosity=3)
