from . import _broker

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
