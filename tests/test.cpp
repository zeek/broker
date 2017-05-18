#define CAF_TEST_NO_MAIN
#include <caf/test/unit_test_impl.hpp>

#include "test.hpp"

#include <caf/io/middleman.hpp>
#include <caf/io/network/test_multiplexer.hpp>

using namespace caf;
using namespace broker;

base_fixture::base_fixture(bool fake_network)
    : ctx(make_config(fake_network)),
      sys(ctx.system()),
      self(sys),
      sched(dynamic_cast<scheduler_type&>(sys.scheduler())) {
  // nop
}

configuration base_fixture::make_config(bool fake_network) {
  configuration cfg;
  cfg.middleman_detach_utility_actors = false;
  cfg.scheduler_policy = caf::atom("testing");
  cfg.logger_verbosity = caf::atom("TRACE");
  if (fake_network)
    cfg.load<io::middleman, io::network::test_multiplexer>();
  return cfg;
}

fake_network_fixture::fake_network_fixture() : base_fixture(true) {
  // nop
}

int main(int argc, char** argv) {
  //if (! broker::logger::file(broker::logger::debug, "broker-unit-test.log"))
  //  return 1;
  return test::main(argc, argv);
}
