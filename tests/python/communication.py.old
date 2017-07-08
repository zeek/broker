import unittest

import broker

class TestCommunication(unittest.TestCase):
  def test_endpoint_spawn(self):
    ctx = broker.Context()
    ctx.spawn(broker.Blocking)
    ctx.spawn(broker.Nonblocking)
    with self.assertRaises(TypeError):
      ctx.spawn("yikes")

  def test_endpoint_subscription_forwarding(self):
    ctx = broker.Context()
    x = ctx.spawn(broker.Blocking)
    y = ctx.spawn(broker.Blocking)
    x.peer(y)
    s = x.receive(broker.Status)
    print(s)
    #x.publish("foo", 4) # nobody subscribed yet
    #x.subscribe("foo")
    #x.publish("foo", 2) # forward to peer
    #x.unsubscribe("foo")
    #x.publish("foo", 0) # no more

  def test_endpoint_blocking_local_subscription(self):
    ctx = broker.Context()
    ep = ctx.spawn(broker.Blocking)
    ep.subscribe("foo")
    ep.publish("foo.bar", 42)
    ep.publish("foo", [1, 2, 3])
    msg = ep.receive(broker.Message)
    self.assertEqual(msg.topic(), "foo.bar")
    # FIXME: data needs to be unwrapped
    #self.assertEqual(msg.data(), 42)

if __name__ == '__main__':
  unittest.main(verbosity=3)
