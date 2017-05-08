#define CAF_TEST_NO_MAIN
#include <caf/test/unit_test_impl.hpp>

#include "test.hpp"

using namespace caf;
using namespace broker;

configuration test_coordinator_context_fixture::make_config() {
  configuration cfg;
  cfg.scheduler_policy = caf::atom("testing");
  cfg.logger_verbosity = caf::atom("TRACE");
  return cfg;
}

test_coordinator_context_fixture::test_coordinator_context_fixture()
    : ctx(make_config()),
      sys(ctx.system()),
      self(sys),
      sched(dynamic_cast<scheduler_type&>(sys.scheduler())) {
  // nop
}

int main(int argc, char** argv) {
  //if (! broker::logger::file(broker::logger::debug, "broker-unit-test.log"))
  //  return 1;
  return test::main(argc, argv);
}
