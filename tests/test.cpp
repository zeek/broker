#define CAF_TEST_NO_MAIN
#include <caf/test/unit_test_impl.hpp>

int main(int argc, char** argv) {
  //if (! broker::logger::file(broker::logger::debug, "broker-unit-test.log"))
  //  return 1;
  return caf::test::main(argc, argv);
}
