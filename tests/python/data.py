from broker import *
from datetime import *
from ipaddress import *

# TODO: figure out a better way to test correctness.
def check_data_ctor(x):
  print(Data(x))
  assert Data(x) == x

# None
check_data_ctor(None)

# Bool
check_data_ctor(True)
check_data_ctor(False)

# Integer
check_data_ctor(42)
check_data_ctor(-42)

# Count
check_data_ctor(Count(42))

# Real
check_data_ctor(4.2)
check_data_ctor(-4.2)

# Interval
check_data_ctor(Interval(1234))
check_data_ctor(timedelta(seconds=1, microseconds=1))
check_data_ctor(timedelta(milliseconds=-1))

# Timestamp
check_data_ctor(now())
check_data_ctor(datetime.today())

# String
check_data_ctor('foo')

# Address
check_data_ctor(IPv4Address('1.2.3.4'))
check_data_ctor(IPv6Address('dead::beef'))

# Subnet
check_data_ctor(IPv4Network('10.0.0.0/8'))
check_data_ctor(IPv6Network('::1/128'))

# Port
check_data_ctor(Port(22, Port.TCP))
check_data_ctor(Port(53, Port.UDP))
check_data_ctor(Port(8, Port.ICMP))
check_data_ctor(Port(0, Port.Unknown))

# Vector
check_data_ctor([])
check_data_ctor([1, 2, 3])
check_data_ctor(["foo", [], [[True]], IPv4Address('1.2.3.4'), None])

# Set
check_data_ctor(set())
check_data_ctor(set([1, 2, 3])) # Python 2.7 compatibility syntax
check_data_ctor(set([1, 2, 1])) # Python 2.7 compatibility syntax

# Table
check_data_ctor(dict())
check_data_ctor({'foo': 42, Count(7): True})
check_data_ctor({True: 42, Port(22, Port.TCP): False})
