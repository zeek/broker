#include "broker/internal/meta_data_writer.hh"

#include <caf/binary_serializer.hpp>

#include "broker/internal_command.hh"

namespace broker::internal {

namespace {

class helper {
public:
  explicit helper(caf::binary_serializer& f) : f_(f) {
    // nop
  }

  template <class T>
  bool operator()(const T&) {
    // We only store dynamic type information and container sizes.
    return true;
  }

  bool operator()(const std::string& x) {
    return apply(static_cast<uint32_t>(x.size()));
  }

  bool operator()(const enum_value& x) {
    return (*this)(x.name);
  }

  bool operator()(const set& xs) {
    if (!apply(static_cast<uint32_t>(xs.size())))
      return false;
    for (auto& x : xs)
      if (!(*this)(x))
        return false;
    return true;
  }

  bool operator()(const table& xs) {
    if (!apply(static_cast<uint32_t>(xs.size())))
      return false;
    for (auto& kvp : xs)
      if (!(*this)(kvp.first) || !(*this)(kvp.second))
        return false;
    return true;
  }

  bool operator()(const vector& xs) {
    if (!apply(static_cast<uint32_t>(xs.size())))
      return false;
    for (auto& x : xs)
      if (!(*this)(x))
        return false;
    return true;
  }

  bool operator()(const data& x) {
    return apply(x.get_type())
           && visit([this](const auto& value) { return (*this)(value); }, x);
  }

  bool operator()(const std::vector<sequence_number_type>& x) {
    return apply(static_cast<uint32_t>(x.size()));
  }

  bool operator()(const put_command& x) {
    return (*this)(x.key) && (*this)(x.value);
  }

  bool operator()(const put_unique_command& x) {
    return (*this)(x.key) && (*this)(x.value);
  }

  bool operator()(const erase_command& x) {
    return (*this)(x.key);
  }

  bool operator()(const expire_command& x) {
    return (*this)(x.key);
  }

  bool operator()(const add_command& x) {
    return (*this)(x.key) && (*this)(x.value) && (*this)(x.init_type);
  }

  bool operator()(const subtract_command& x) {
    return (*this)(x.key) && (*this)(x.value);
  }

  bool operator()(const attach_writer_command& x) {
    return (*this)(x.offset) && (*this)(x.heartbeat_interval);
  }

  bool operator()(const keepalive_command& x) {
    return (*this)(x.seq);
  }

  bool operator()(const cumulative_ack_command& x) {
    return (*this)(x.seq);
  }

  bool operator()(const nack_command& x) {
    return (*this)(x.seqs);
  }

  bool operator()(const ack_clone_command& x) {
    return (*this)(x.offset)                //
           && (*this)(x.heartbeat_interval) //
           && (*this)(x.state);
  }

  bool operator()(const retransmit_failed_command& x) {
    return (*this)(x.seq);
  }

  bool operator()(const internal_command& x) {
    return apply(type_of(x))
           && visit([this](auto& value) { return (*this)(value); }, x.content);
  }

  error&& move_error() {
    return std::move(err_);
  }

private:
  template <class T>
  bool apply(T x) {
    if constexpr (std::is_integral_v<T>) {
      if (!f_.value(x)) {
        err_ = f_.get_error();
        return false;
      }
      return true;
    } else {
      static_assert(std::is_enum_v<T>);
      return apply(static_cast<std::underlying_type_t<T>>(x));
    }
  }

  caf::binary_serializer& f_;
  error err_;
};

} // namespace

meta_data_writer::meta_data_writer(caf::binary_serializer& sink) : sink_(sink) {
  // nop
}

error meta_data_writer::operator()(const data& x) {
  helper h{sink_};
  h(x);
  return h.move_error();
}

error meta_data_writer::operator()(const internal_command& x) {
  helper h{sink_};
  h(x);
  return h.move_error();
}

} // namespace broker::internal
