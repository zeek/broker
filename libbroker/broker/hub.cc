#include "hub.hh"

#include "detail/flare.hh"
#include "message.hh"

#include <caf/actor.hpp>

#include <mutex>

namespace broker {

namespace {

/// Stores the last ID that was assigned to a hub. The next ID will be one
/// greater than this value. Hence, valid hub IDs start at 1.
std::atomic<uint64_t> last_hub_id = 0;

} // namespace

class hub::impl {
public:
  std::vector<data_message> poll();

  data_message get();

  std::vector<data_message> get(size_t num);

  size_t available() const noexcept;

  detail::native_socket read_fd() const noexcept;

  detail::native_socket write_fd() const noexcept;

  void subscribe(const topic& x, bool block);

  void unsubscribe(const topic& x, bool block);

  void publish(const topic& dest, set_builder&& content);

  void publish(const topic& dest, table_builder&& content);

  void publish(const topic& dest, list_builder&& content);

private:
  caf::actor core_;
};

hub_id hub::next_id() noexcept {
  return static_cast<hub_id>(++last_hub_id);
}

std::vector<data_message> hub::poll() {
  return impl_->poll();
}

data_message hub::get() {
  return impl_->get();
}

std::vector<data_message> hub::get(size_t num) {
  return impl_->get(num);
}

size_t hub::available() const noexcept {
  return impl_->available();
}

detail::native_socket hub::read_fd() const noexcept {
  return impl_->read_fd();
}

detail::native_socket hub::write_fd() const noexcept {
  return impl_->write_fd();
}

void hub::subscribe(const topic& x, bool block) {
  impl_->subscribe(x, block);
}

void hub::unsubscribe(const topic& x, bool block) {
  impl_->unsubscribe(x, block);
}

void hub::publish(const topic& dest, set_builder&& content) {
  impl_->publish(dest, std::move(content));
}

void hub::publish(const topic& dest, table_builder&& content) {
  impl_->publish(dest, std::move(content));
}

void hub::publish(const topic& dest, list_builder&& content) {
  impl_->publish(dest, std::move(content));
}

} // namespace broker
