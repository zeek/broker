#pragma once

#include <future>

#include <caf/async/fwd.hpp>
#include <caf/flow/fwd.hpp>

#include "broker/fwd.hh"

namespace broker::detail {

class flow_controller {
public:
  // -- member types -----------------------------------------------------------

  template <class T>
  using source_queue = shared_publisher_queue<T>;

  template <class T>
  using source_queue_ptr = shared_publisher_queue_ptr<T>;

  template <class T>
  using source_queue_promise = std::promise<source_queue_ptr<T>>;

  template <class T>
  using source_queue_promise_ptr = std::shared_ptr<source_queue_promise<T>>;

  template <class T>
  using sink_queue = shared_subscriber_queue<T>;

  template <class T>
  using sink_queue_ptr = shared_subscriber_queue_ptr<T>;

  template <class T>
  using sink_queue_promise = std::promise<sink_queue_ptr<T>>;

  template <class T>
  using sink_queue_promise_ptr = std::shared_ptr<sink_queue_promise<T>>;

  // -- constructors, destructors, and assignment operators --------------------

  virtual ~flow_controller();

  // -- setup functions --------------------------------------------------------

  /// @pre `promise != nullptr`
  void connect(source_queue_promise_ptr<data_message> promise);

  /// @pre `promise != nullptr`
  void connect(sink_queue_promise_ptr<data_message> promise,
               const filter_type& filter);

  /// @note calls `add_filter(filter)`.
  void update_filter(sink_queue_ptr<data_message> queue,
                     const filter_type& filter);

  // -- interface functions ----------------------------------------------------

  virtual caf::scheduled_actor* ctx() = 0;

  virtual void add_source(caf::flow::observable<data_message> source) = 0;

  virtual void add_source(caf::flow::observable<command_message> source) = 0;

  virtual void add_sink(caf::flow::observer<data_message> sink) = 0;

  virtual void add_sink(caf::flow::observer<command_message> sink) = 0;

  virtual caf::async::publisher<data_message>
  select_local_data(const filter_type& filter) = 0;

  virtual caf::async::publisher<command_message>
  select_local_commands(const filter_type& filter) = 0;

  /// Extends the global filter with all topics from `filter`.
  virtual void add_filter(const filter_type& filter) = 0;
};

} // namespace broker::detail
