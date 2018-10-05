#define CAF_TEST_NO_MAIN
#include <caf/test/unit_test_impl.hpp>

#include "test.hpp"

#include <caf/defaults.hpp>
#include <caf/io/middleman.hpp>
#include <caf/io/network/test_multiplexer.hpp>

using namespace caf;
using namespace broker;

base_fixture::base_fixture(bool fake_network)
    : ep(make_config(fake_network)),
      sys(ep.system()),
      self(sys),
      sched(dynamic_cast<scheduler_type&>(sys.scheduler())),
      credit_round_interval(get_or(sys.config(),
                            "stream.credit-round-interval",
                            caf::defaults::stream::credit_round_interval)) {
  // nop
}

base_fixture::~base_fixture() {
  run();
  // Our core might do some messaging in its dtor, hence we need to make sure
  // messages are handled when enqueued to avoid blocking.
  sched.inline_all_enqueues();
}

configuration base_fixture::make_config(bool fake_network) {
  broker_options options;
  options.disable_ssl = fake_network;
  configuration cfg{options};
  cfg.set("scheduler.policy", caf::atom("testing"));
  cfg.set("logger.verbosity", caf::atom("TRACE"));
  cfg.set("middleman.attach-utility-actors", true);
  cfg.parse(test::engine::argc(), test::engine::argv());
  if (fake_network)
    cfg.load<io::middleman, io::network::test_multiplexer>();
  return cfg;
}

void base_fixture::run() {
  while (sched.has_job() || sched.has_pending_timeout()) {
    sched.run();
    sched.trigger_timeouts();
  }
}

void base_fixture::consume_message() {
  if (!sched.try_run_once())
    CAF_FAIL("no message to consume");
}

fake_network_fixture::fake_network_fixture() : base_fixture(true) {
  // nop
}

int main(int argc, char** argv) {
  //if (! broker::logger::file(broker::logger::debug, "broker-unit-test.log"))
  //  return 1;
  return test::main(argc, argv);
}
