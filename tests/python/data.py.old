import datetime
import ipaddress
import unittest

import broker
import broker._broker as _broker

class TestDataConstruction(unittest.TestCase):

  def compare_data_to_internal(self, x):
    lhs = broker.Data(x).get()
    rhs = _broker.Data(x)
    print("raw: %s, data: %s" % (x, rhs))
    self.assertEqual(lhs, rhs)

  def test_none(self):
    self.assertEqual(broker.Data().get(), _broker.Data())
    self.assertEqual(broker.Data(None).get(), _broker.Data())

  def test_bool(self):
    self.compare_data_to_internal(True)
    self.compare_data_to_internal(False)

  def test_integer(self):
    self.compare_data_to_internal(42)
    self.compare_data_to_internal(-42)

  def test_count(self):
    self.compare_data_to_internal(broker.Count(42))

  def test_count_overflow(self):
    # TODO: figure out why catching OverflowError doesn't work
    with self.assertRaises(SystemError):
      broker.Count(-1)

  def test_real(self):
    self.compare_data_to_internal(4.2)
    self.compare_data_to_internal(-4.2)

  def test_timespan(self):
    # Timespan only
    to_us = lambda x: x.microseconds + (x.seconds + x.days * 24 * 3600) * 10**6
    to_ns = lambda x: to_us(x) * 10**3
    self.assertEqual(-10**6, to_ns(datetime.timedelta(microseconds = -1000)))
    td = datetime.timedelta(milliseconds = 1, microseconds = 1)
    self.assertEqual(broker.Timespan(10**3 + 10**6), broker.Timespan(to_ns(td)))
    # Data
    neg42 = datetime.timedelta(milliseconds = -42 * 10**3)
    self.compare_data_to_internal(broker.Timespan(to_ns(neg42)))
    self.compare_data_to_internal(broker.Timespan(to_ns(td)))

  def test_timestamp(self):
    self.compare_data_to_internal(broker.now())
    today = datetime.datetime.today()
    time_since_epoch = (today - datetime.datetime(1970, 1, 1)).total_seconds()
    self.compare_data_to_internal(broker.Timestamp(time_since_epoch))

  def test_string(self):
    self.compare_data_to_internal('')
    self.compare_data_to_internal('foo')

  def test_address_v4(self):
    addr = ipaddress.IPv4Address('1.2.3.4')
    self.assertEqual(broker.Data(addr), _broker.Data(_broker.Address(addr.packed, 4)))

  def test_address_v6(self):
    addr = ipaddress.IPv6Address('::1')
    self.assertEqual(broker.Data(addr), _broker.Data(_broker.Address(addr.packed, 6)))

  def test_subnet_v4(self):
    sn = ipaddress.IPv4Network('10.0.0.0/8')
    addr = _broker.Address(sn.network_address.packed, 4)
    self.assertEqual(broker.Data(sn), _broker.Data(_broker.Subnet(addr, sn.prefixlen)))

  def test_subnet_v6(self):
    sn = ipaddress.IPv6Network('::1/128')
    addr = _broker.Address(sn.network_address.packed, 6)
    self.assertEqual(broker.Data(sn), _broker.Data(_broker.Subnet(addr, sn.prefixlen)))

  def test_port(self):
    self.compare_data_to_internal(broker.Port(22, broker.Port.TCP))
    self.compare_data_to_internal(broker.Port(53, broker.Port.UDP))
    self.compare_data_to_internal(broker.Port(8, broker.Port.ICMP))
    self.compare_data_to_internal(broker.Port(0, broker.Port.Unknown))

  def test_vector(self):
    l = []
    self.assertEqual(str(broker.Data(l)), "[]")
    l = [1, 2, 3]
    self.assertEqual(str(broker.Data(l)), "[1, 2, 3]")
    l = ["foo", [[True]], ipaddress.IPv6Address('::1'), None]
    self.assertEqual(str(broker.Data(l)), "[foo, [[T]], ::1, nil]")

  def test_set(self):
    s = set()
    self.assertEqual(str(broker.Data(s)), "{}")
    s = set([1, 2, 3])
    self.assertEqual(str(broker.Data(s)), "{1, 2, 3}")

  def test_table(self):
    d = dict()
    self.assertEqual(str(broker.Data(d)), "{}") # FIXME: disambiguate from set
    d = {'foo': 42, broker.Count(7): True}
    self.assertEqual(str(broker.Data(d)), "{7 -> T, foo -> 42}")
    d = {True: 42, broker.Port(22, broker.Port.TCP): False}
    self.assertEqual(str(broker.Data(d)), "{T -> 42, 22/tcp -> F}")

if __name__ == '__main__':
  unittest.main(verbosity=3)
