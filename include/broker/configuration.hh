#pragma once

#include <caf/actor_system_config.hpp>

#include "broker/defaults.hh"

namespace broker {

struct broker_options {
  /// If true, peer connections won't use SSL.
  bool disable_ssl = false;

  /// If true, endpoints will forward incoming messages to peers.
  bool forward = true;

  /// TTL to insert into forwarded messages. Messages will be droppped once
  /// they have traversed more than this many hops. Note that the 1st
  /// receiver inserts the TTL (not the sender!). The 1st receiver does
  /// already count against the TTL.
  unsigned int ttl = defaults::ttl;

  /// Whether to use real/wall clock time for data store time-keeping
  /// tasks or whether the application will simulate time on its own.
  bool use_real_time = true;

  /// Whether to ignore the `broker.conf` file.
  bool ignore_broker_conf = false;

  broker_options() = default;

  broker_options(const broker_options&) = default;

  broker_options& operator=(const broker_options&) = default;
};

/// Configures an ::endpoint.
///
/// The configuration draws user-provided options from three sources (in order):
/// 1. The file `broker.conf`. Contents of this file override hard-coded
///    defaults. Broker only scans the current working directory when looking
///    for this file.
/// 2. Environment variables. Broker currently recognizes the following
///    environment variables:
///    - `BROKER_CONSOLE_VERBOSITY`: enables console output by overriding
///      `logger.console-verbosity`. Valid values are `trace`, `debug`, `info`,
///      `warning`, and `error`.
///    - `BROKER_FILE_VERBOSITY`: enables log file output by overriding
///      `logger.file-verbosity`.
///    - `BROKER_RECORDING_DIRECTORY` enables recording of meta data for the
///      `broker-cluster-benchmark` tool.
///    - `BROKER_OUTPUT_GENERATOR_FILE_CAP` restricts the number of recorded
///      messages in recording mode.
/// 3. Command line arguments (if provided).
///
/// As a rule of thumb, set `BROKER_CONSOLE_VERBOSITY` to `info` for getting
/// output on high-level events such as peerings. If you need to tap
/// into published messages, set `BROKER_CONSOLE_VERBOSITY` to `debug`. Enabling
/// debug output will slow down Broker and generates a lot of console output.
///
/// Writing to a file instead of printing to the command line can help grepping
/// through large logs or correlating logs from multiple Broker peers.
class configuration : public caf::actor_system_config {
public:
  using super = caf::actor_system_config;

  struct skip_init_t {};

  static constexpr skip_init_t skip_init = skip_init_t{};

  configuration();

  configuration(configuration&&) = default;

  /// Constructs a configuration with non-default Broker options.
  explicit configuration(broker_options opts);

  /// Constructs a configuration from command line arguments.
  configuration(int argc, char** argv);

  /// Returns default Broker options and flags.
  const broker_options& options() const {
    return options_;
  }

  caf::settings dump_content() const override;

  /// Adds all Broker message types to `cfg`.
  /// @note this function has no effect when compiling against CAF ≥ 0.18
  static void add_message_types(caf::actor_system_config& cfg);

  /// Initializes any global state required by Broker such as the global meta
  /// object table for Broker and CAF (core, I/O and OpenSSL modules). This
  /// function is safe to call multiple times (repeated calls have no effect).
  /// @note this function has no effect when compiling against CAF < 0.18
  /// @note all constructors call this function implicitly, but users can call
  ///       it explicitly when using a custom config class or when calling CAF
  ///       code prior to creating the configuration object.
  static void init_global_state();

protected:
  /// Allows subtypes to add custom options before the configuration reads
  /// `broker.conf` or command line arguments. Requires the subtype to call
  /// `init` manually.
  explicit configuration(skip_init_t);

  void init(int argc, char** argv);

private:
  broker_options options_;
};

} // namespace broker
