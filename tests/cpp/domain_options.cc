#define SUITE domain_options

#include "broker/domain_options.hh"

#include "test.hh"

#include <caf/settings.hpp>

using namespace broker;

TEST(domain options can save their values in settings) {
  using caf::get_or;
  caf::settings xs;
  domain_options opts;
  opts.disable_forwarding = true;
  opts.save(xs);
  CHECK_EQUAL(get_or(xs, "broker.disable-forwarding", false), true);
}

TEST(domain options can load their values from settings) {
  using caf::get_or;
  caf::settings xs;
  caf::put(xs, "broker.disable-forwarding", true);
  domain_options opts;
  CHECK_EQUAL(opts.disable_forwarding, false);
  opts.load(xs);
  CHECK_EQUAL(opts.disable_forwarding, true);
}
