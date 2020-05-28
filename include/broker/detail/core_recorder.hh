#pragma once

#include <fstream>

#include <caf/fwd.hpp>

#include "broker/detail/assert.hh"
#include "broker/detail/generator_file_writer.hh"
#include "broker/filter_type.hh"
#include "broker/logger.hh"
#include "broker/message.hh"

namespace broker::detail {

class core_recorder {
public:
  explicit core_recorder(caf::local_actor* self);

  void record_subscription(const filter_type& what);

  void record_peer(const caf::node_id& peer_id);

  explicit operator bool() const noexcept {
    return writer_ != nullptr;
  }

  bool operator!() const noexcept {
    return !writer_;
  }

  size_t remaining_records() const noexcept {
    return remaining_records_ ;
  }

  template <class T>
  bool try_record(const T& x) {
    BROKER_ASSERT(writer_ != nullptr);
    BROKER_ASSERT(remaining_records_ > 0);
    if (auto err = writer_->write(x)) {
      BROKER_WARNING("unable to write to generator file:" << err);
      writer_ = nullptr;
      remaining_records_ = 0;
      return false;
    }
    if (--remaining_records_ == 0) {
      BROKER_DEBUG("reached recording cap, close file");
      writer_ = nullptr;
    }
    return true;
  }

  bool try_record(const node_message& x) {
    return try_record(get_content(x));
  }

private:
  bool open_file(std::ofstream& fs, std::string file_name);

  /// Helper for recording meta data of published messages.
  detail::generator_file_writer_ptr writer_;

  /// Counts down when using a `recorder_` to cap maximum file entries.
  size_t remaining_records_ = 0;

  /// Handle for recording all subscribed topics.
  std::ofstream topics_file_;

  /// Handle for recording all peers.
  std::ofstream peers_file_;
};

} // namespace broker::detail
