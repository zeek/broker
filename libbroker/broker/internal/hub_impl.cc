#include "broker/internal/hub_impl.hh"

namespace broker::internal {

hub_impl::~hub_impl() {
  if (write_queue_) {
    write_queue_->close();
  }
  if (read_queue_) {
    read_queue_->cancel();
  }
}

std::vector<data_message> hub_impl::poll() {
  // The Queue may return a capacity of 0 if the producer has closed the flow.
  std::vector<data_message> buf;
  auto max_size = read_queue_->capacity();
  if (max_size > 0) {
    buf.reserve(max_size);
    read_queue_->pull(buf, max_size);
  }
  return buf;
}

data_message hub_impl::get() {
  data_message msg;
  if (!read_queue_->pull(msg)) {
    throw std::runtime_error("subscriber queue closed");
  }
  return msg;
}

std::vector<data_message> hub_impl::get(size_t num) {
  std::vector<data_message> buf;
  if (num == 0) {
    return buf;
  }
  buf.reserve(num);
  read_queue_->pull(buf, num);
  while (buf.size() < num) {
    read_queue_->wait();
    if (!read_queue_->pull(buf, num))
      return buf;
  }
  return buf;
}

std::vector<data_message> hub_impl::get(size_t num, timestamp timeout) {
  std::vector<data_message> buf;
  if (num == 0) {
    return buf;
  }
  buf.reserve(num);
  read_queue_->pull(buf, num);
  while (buf.size() < num) {
    if (!read_queue_->wait_until(timeout)) {
      return buf; // Timeout occurred, return what we have.
    }
    if (!read_queue_->pull(buf, num))
      return buf;
  }
  return buf;
}

data_message hub_impl::get(timestamp timeout) {
  data_message msg;
  if (read_queue_->wait_until(timeout)) {
    read_queue_->pull(msg);
  }
  return msg;
}

} // namespace broker::internal
