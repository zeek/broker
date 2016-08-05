from . import _broker

import datetime
import ipaddress

#
# Version & Contants
#

VERSION_MAJOR = _broker.Version.MAJOR
VERSION_MINOR = _broker.Version.MINOR
VERSION_PATCH = _broker.Version.PATCH
VERSION_PROTOCOL = _broker.Version.PROTOCOL

VERSION = '%u.%u.%u' % (VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH)

def compatible(protocol_version):
  _broker.Version.compatible(protocol_version)

EC = _broker.EC
PeerStatus = _broker.PeerStatus
PeerFlags = _broker.PeerFlags
ApiFlags = _broker.ApiFlags
Frontend = _broker.Frontend
Backend = _broker.Backend
Error = _broker.Error
EndpointInfo = _broker.EndpointInfo
Message = _broker.Message
NetworkInfo = _broker.NetworkInfo
PeerInfo = _broker.PeerInfo
Status = _broker.Status

#
# Data Model
#

Count = _broker.Count
Interval = _broker.Interval
Timestamp = _broker.Timestamp
Port = _broker.Port

def now():
  return _broker.now()

class Data:
  def __init__(self, x = None):
    if x is None:
      self.data = _broker.Data()
    elif isinstance(x, (bool, int, float, str, Count, Interval, Timestamp,
                        Port)):
      self.data = _broker.Data(x)
    elif isinstance(x, ipaddress.IPv4Address):
      self.data = _broker.Data(_broker.Address(x.packed, 4))
    elif isinstance(x, ipaddress.IPv6Address):
      self.data = _broker.Data(_broker.Address(x.packed, 6))
    elif isinstance(x, ipaddress.IPv4Network):
      address = _broker.Address(x.network_address.packed, 4)
      length = x.prefixlen
      self.data = _broker.Data(_broker.Subnet(address, length))
    elif isinstance(x, ipaddress.IPv6Network):
      address = _broker.Address(x.network_address.packed, 6)
      length = x.prefixlen
      self.data = _broker.Data(_broker.Subnet(address, length))
    elif isinstance(x, list):
      v = _broker.Vector(list(map(lambda d: Data(d).get(), x)))
      self.data = _broker.Data(v)
    elif isinstance(x, set):
      s = _broker.Set(list(map(lambda d: Data(d).get(), x)))
      self.data = _broker.Data(s)
    elif isinstance(x, dict):
      t = _broker.Table({Data(k).get(): Data(v).get() for k, v in x.items()})
      self.data = _broker.Data(t)
    else:
      raise Exception("unsupported data type")

  def get(self):
    return self.data

  def __eq__(self, other):
    if isinstance(other, Data):
      return self.data == other.data
    else:
      return self == Data(other)

  def __ne__(self, other):
    return not self.__eq__(other)

  def __str__(self):
    return str(self.data)
