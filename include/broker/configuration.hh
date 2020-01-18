#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include <caf/actor_system_config.hpp>
#pragma GCC diagnostic pop

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
  unsigned int ttl = 20;
  /// Whether to use real/wall clock time for data store time-keeping
  /// tasks or whether the application will simulate time on its own.
  bool use_real_time = true;

  broker_options() {}
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

  /// Default-constructs a configuration.
  configuration();

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
  static void add_message_types(caf::actor_system_config& cfg);

private:
  void init(int argc, char** argv);

  broker_options options_;
};

} // namespace broker
