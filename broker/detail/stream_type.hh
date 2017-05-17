#ifndef BROKER_DETAIL_STREAM_TYPE_HH
#define BROKER_DETAIL_STREAM_TYPE_HH

#include <utility>

#include <caf/stream.hpp>

#include "broker/data.hh"
#include "broker/topic.hh"

namespace broker {
namespace detail {

using stream_type = caf::stream<std::pair<topic, data>>;

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_STREAM_TYPE_HH
