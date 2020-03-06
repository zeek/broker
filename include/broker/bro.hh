#pragma once

#pragma message("Warning: bro.hh header is deprecated, use zeek.hh instead")

#include "broker/zeek.hh"

namespace broker {
namespace bro {

using Message
      [[deprecated("use version from zeek.hh and zeek namespace instead")]]
      = broker::zeek::Message;

using Event
      [[deprecated("use version from zeek.hh and zeek namespace instead")]]
      = broker::zeek::Event;

using Batch
      [[deprecated("use version from zeek.hh and zeek namespace instead")]]
      = broker::zeek::Batch;

using LogCreate
      [[deprecated("use version from zeek.hh and zeek namespace instead")]]
      = broker::zeek::LogCreate;

using LogWrite
      [[deprecated("use version from zeek.hh and zeek namespace instead")]]
      = broker::zeek::LogWrite;

using IdentifierUpdate
      [[deprecated("use version from zeek.hh and zeek namespace instead")]]
      = broker::zeek::IdentifierUpdate;

} // namespace broker
} // namespace bro
