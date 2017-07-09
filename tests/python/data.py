#
# Testing the data conversion back and forth between broker.Data and Python.
# Note that the user will rarely see broker.Data instances directly. We make
# sure he that the transparent conversion works as expected.
#

import datetime
import ipaddress
import sys
import unittest

import broker

class TestDataConstruction(unittest.TestCase):
    def check_to_broker(self, x, s, t):
        assert not isinstance(x, broker.Data)
        y = broker.Data(x)
        #print("[to_broker] ({} / {}) -> ({} / {}) (expected: {} / {})".format(x, type(x), str(y), y.get_type(), s, t))

        if s is not None:
            self.assertEqual(str(y), s)

        self.assertEqual(y.get_type(), t)
        return y

    def check_to_py(self, x, s):
        y = broker.Data.to_py(x)
        #print("[to_py] data({} / {}) -> ({} / {})".format(str(x), x.get_type(), y, type(y)))
        self.assertIsInstance(y, type(s))
        self.assertEqual(y, s)
        return y

    def check_to_broker_and_back(self, x, s, t):
        d = self.check_to_broker(x, s, t)
        self.check_to_py(d, x)
        return d

    def test_bool(self):
        self.check_to_broker_and_back(True, 'T', broker.Data.Type.Boolean)
        self.check_to_broker_and_back(False, 'F', broker.Data.Type.Boolean)

    def test_integer(self):
        self.check_to_broker_and_back(42, '42', broker.Data.Type.Integer)
        self.check_to_broker_and_back(-42, '-42', broker.Data.Type.Integer)

    def test_count(self):
        self.check_to_broker_and_back(broker.Count(42), '42', broker.Data.Type.Count)

    def test_count_overflow(self):
        with self.assertRaises(OverflowError) as context:
            self.check_to_broker(broker.Count(-1), '-1', broker.Data.Type.Count)

    def test_real(self):
        self.check_to_broker_and_back(4.2, '4.200000', broker.Data.Type.Real)
        self.check_to_broker_and_back(-4.2, '-4.200000', broker.Data.Type.Real)

    def test_timespan(self):
        # Timespan only
        to_us = lambda x: x.microseconds + (x.seconds + x.days * 24 * 3600) * 10**6
        to_ns = lambda x: to_us(x) * 10**3
        self.assertEqual(-10**6, to_ns(datetime.timedelta(microseconds = -1000)))
        td = datetime.timedelta(milliseconds = 1, microseconds = 1)
        self.assertEqual(broker.Timespan(10**3 + 10**6), broker.Timespan(to_ns(td)))
        # Data
        neg42 = datetime.timedelta(milliseconds = -42 * 10**3)
        self.check_to_broker_and_back(broker.Timespan(to_ns(neg42)), '-42000000000ns', broker.Data.Type.Timespan)
        self.check_to_broker_and_back(broker.Timespan(to_ns(td)), '1001000ns', broker.Data.Type.Timespan)

    def test_timestamp(self):
        self.check_to_broker(broker.now(), None, broker.Data.Type.Timestamp)
        today = datetime.datetime.today()
        time_since_epoch = (today - datetime.datetime(1970, 1, 1)).total_seconds()
        self.check_to_broker_and_back(broker.Timestamp(time_since_epoch), None, broker.Data.Type.Timestamp)

    def test_string(self):
        self.check_to_broker_and_back('', '', broker.Data.Type.String)
        self.check_to_broker_and_back('foo', 'foo', broker.Data.Type.String)

    def test_address_v4(self):
        addr = ipaddress.IPv4Address('1.2.3.4')
        self.check_to_broker_and_back(addr, '1.2.3.4', broker.Data.Type.Address)

    def test_address_v6(self):
        addr = ipaddress.IPv6Address('::1')
        self.check_to_broker_and_back(addr, '::1', broker.Data.Type.Address)

    def test_subnet_v4(self):
        sn = ipaddress.IPv4Network('10.0.0.0/8')
        self.check_to_broker_and_back(sn, '10.0.0.0/8', broker.Data.Type.Subnet)

    def test_subnet_v6(self):
        sn = ipaddress.IPv6Network('::1/128')
        self.check_to_broker_and_back(sn, '::1/128', broker.Data.Type.Subnet)

    def test_port(self):
        self.check_to_broker_and_back(broker.Port(22, broker.Port.TCP), "22/tcp", broker.Data.Type.Port)
        self.check_to_broker_and_back(broker.Port(53, broker.Port.UDP), "53/udp", broker.Data.Type.Port)
        self.check_to_broker_and_back(broker.Port(8, broker.Port.ICMP), "8/icmp", broker.Data.Type.Port)
        self.check_to_broker_and_back(broker.Port(0, broker.Port.Unknown), "0/?", broker.Data.Type.Port)

    def test_set(self):
        d = set([1, 2, 3])
        d = self.check_to_broker_and_back(d, '{1, 2, 3}', broker.Data.Type.Set)

        for (i, x) in enumerate(d.as_set()):
            self.check_to_broker(x, str(i + 1), broker.Data.Type.Integer)
            self.check_to_py(x, i + 1)

        d = broker.Data(set(['foo', ipaddress.IPv6Address('::1'), None]))
        v = d.as_set()
        for (i, x) in enumerate(d.as_set()):
            if i == 1:
                self.check_to_broker(x, 'foo', broker.Data.Type.String)
                self.check_to_py(x, 'foo')
            if i == 2:
                self.check_to_broker(x, '::1', broker.Data.Type.Address)
                self.check_to_py(x, ipaddress.IPv6Address('::1'))

        # Test some of our own methods on wrapped sets.
        d = broker.Data(set([1, 2, 3])).as_set()
        self.assertEqual(str(d), "Set{1, 2, 3}")
        d.remove(broker.Data(2))
        self.assertEqual(str(d), "Set{1, 3}")
        d.clear()
        self.assertEqual(str(d), "Set{}")

    def test_table(self):
        d = {"a": 1, "b": 2, "c": 3}
        d = self.check_to_broker_and_back(d, '{a -> 1, b -> 2, c -> 3}', broker.Data.Type.Table)

        for (i, (k, v)) in enumerate(d.as_table().items()):
            self.check_to_broker(k, ["a", "b", "c"][i], broker.Data.Type.String)
            self.check_to_py(k, ["a", "b", "c"][i])
            self.check_to_broker(v, str(i + 1), broker.Data.Type.Integer)
            self.check_to_py(v, i + 1)

        d = {True: 42, broker.Port(22, broker.Port.TCP): False}
        d = self.check_to_broker(d, '{T -> 42, 22/tcp -> F}', broker.Data.Type.Table)

        t = d.as_table()
        self.check_to_broker(t[broker.Data(True)], "42", broker.Data.Type.Integer)
        self.check_to_py(t[broker.Data(True)], 42)
        self.check_to_broker(t[broker.Data(broker.Port(22, broker.Port.TCP))], "F", broker.Data.Type.Boolean)
        self.check_to_py(t[broker.Data(broker.Port(22, broker.Port.TCP))], False)

        self.check_to_broker_and_back({}, '{}', broker.Data.Type.Table)

    def test_vector(self):
        d = self.check_to_broker([1, 2, 3], '[1, 2, 3]', broker.Data.Type.Vector)

        for (i, x) in enumerate(d.as_vector()):
            self.check_to_broker(x, str(i + 1), broker.Data.Type.Integer)
            self.check_to_py(x, i + 1)

        d = broker.Data(['foo', [[True]], ipaddress.IPv6Address('::1'), None])
        v = d.as_vector()
        self.check_to_broker(v[0], 'foo', broker.Data.Type.String)
        self.check_to_py(v[0], 'foo')

        v1 = v[1].as_vector()
        self.check_to_broker(v1, '[[T]]', broker.Data.Type.Vector)
        self.check_to_py(v[1], [[True]])
        self.check_to_broker(v1[0], '[T]', broker.Data.Type.Vector)
        self.check_to_py(v1[0], [True])

        self.check_to_broker(v[2], '::1', broker.Data.Type.Address)
        self.check_to_py(v[2], ipaddress.IPv6Address("::1"))
        self.check_to_broker(v[3], 'nil', broker.Data.Type.Nil)

        self.check_to_broker_and_back([], '[]', broker.Data.Type.Vector)

if __name__ == '__main__':
    unittest.main(verbosity=3)
