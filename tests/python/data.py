import unittest

from broker import *
import broker._broker as _broker

from datetime import *
from ipaddress import *

class TestDataConstruction(unittest.TestCase):

  def compare_data_to_interal(self, x):
    lhs = Data(x).get()
    rhs = _broker.Data(x)
    print("raw: %s, data: %s" % (x, rhs))
    self.assertEqual(lhs, rhs)

  def test_none(self):
    self.assertEqual(Data().get(), _broker.Data())
    self.assertEqual(Data(None).get(), _broker.Data())

  def test_bool(self):
    self.compare_data_to_interal(True)
    self.compare_data_to_interal(False)

  def test_integer(self):
    self.compare_data_to_interal(42)
    self.compare_data_to_interal(-42)

  def test_count(self):
    self.compare_data_to_interal(Count(42))

  def test_count_overflow(self):
    # TODO: figure out why catching OverflowError doesn't work
    with self.assertRaises(SystemError):
      Count(-1)

  def test_real(self):
    self.compare_data_to_interal(4.2)
    self.compare_data_to_interal(-4.2)

  def test_interval(self):
    # Interval only
    to_us = lambda x: x.microseconds + (x.seconds + x.days * 24 * 3600) * 10**6
    to_ns = lambda x: to_us(x) * 10**3
    self.assertEqual(-10**6, to_ns(timedelta(microseconds = -1000)))
    td = timedelta(milliseconds = 1, microseconds = 1)
    self.assertEqual(Interval(10**3 + 10**6), Interval(to_ns(td)))
    # Data
    neg42 = timedelta(milliseconds = -42 * 10**3)
    self.compare_data_to_interal(Interval(to_ns(neg42)))
    self.compare_data_to_interal(Interval(to_ns(td)))

  def test_timestamp(self):
    self.compare_data_to_interal(now())
    today = datetime.today()
    time_since_epoch = (today - datetime(1970, 1, 1)).total_seconds()
    self.compare_data_to_interal(Timestamp(time_since_epoch))

  def test_string(self):
    self.compare_data_to_interal('')
    self.compare_data_to_interal('foo')

  def test_address_v4(self):
    addr = IPv4Address('1.2.3.4')
    self.assertEqual(Data(addr), _broker.Data(_broker.Address(addr.packed, 4)))

  def test_address_v6(self):
    addr = IPv6Address('::1')
    self.assertEqual(Data(addr), _broker.Data(_broker.Address(addr.packed, 6)))

  def test_subnet_v4(self):
    sn = IPv4Network('10.0.0.0/8')
    addr = _broker.Address(sn.network_address.packed, 4)
    self.assertEqual(Data(sn), _broker.Data(_broker.Subnet(addr, sn.prefixlen)))

  def test_subnet_v6(self):
    sn = IPv6Network('::1/128')
    addr = _broker.Address(sn.network_address.packed, 6)
    self.assertEqual(Data(sn), _broker.Data(_broker.Subnet(addr, sn.prefixlen)))

  def test_port(self):
    self.compare_data_to_interal(Port(22, Port.TCP))
    self.compare_data_to_interal(Port(53, Port.UDP))
    self.compare_data_to_interal(Port(8, Port.ICMP))
    self.compare_data_to_interal(Port(0, Port.Unknown))

  def test_vector(self):
    l = []
    self.assertEqual(str(Data(l)), "[]")
    l = [1, 2, 3]
    self.assertEqual(str(Data(l)), "[1, 2, 3]")
    l = ["foo", [[True]], IPv6Address('::1'), None]
    self.assertEqual(str(Data(l)), "[foo, [[T]], ::1, nil]")

  def test_set(self):
    s = set()
    self.assertEqual(str(Data(s)), "{}")
    s = set([1, 2, 3])
    self.assertEqual(str(Data(s)), "{1, 2, 3}")

  def test_table(self):
    d = dict()
    self.assertEqual(str(Data(d)), "{}") # FIXME: disambiguate from set
    d = {'foo': 42, Count(7): True}
    self.assertEqual(str(Data(d)), "{7 -> T, foo -> 42}")
    d = {True: 42, Port(22, Port.TCP): False}

if __name__ == '__main__':
  unittest.main(verbosity=3)
