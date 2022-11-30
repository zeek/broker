#define SUITE internal.killswitch

#include "broker/internal/killswitch.hh"

#include "test.hh"

#include <caf/flow/observable_builder.hpp>
#include <caf/flow/scoped_coordinator.hpp>

using namespace broker;

using namespace std::literals;

namespace {

using ivec = std::vector<int>;

struct fixture : test_coordinator_fixture<> {
  caf::flow::scoped_coordinator_ptr ctx = caf::flow::make_scoped_coordinator();
};

} // namespace

BEGIN_FIXTURE_SCOPE(fixture)

SCENARIO("a killswitch allows disrupting a flow at any point in time") {
  GIVEN("an source producing integer values") {
    WHEN("inserting a killswitch via add_killswitch_t") {
      THEN("the killswitch allows us to cancel the subscription mid-flow") {
        auto snk = caf::flow::make_passive_observer<int>();
        auto [in, ks] = ctx //
                          ->make_observable()
                          .repeat(42)
                          .compose(internal::add_killswitch_t{});
        in.subscribe(snk->as_observer());
        snk->sub.request(3);
        ctx->run();
        CHECK_EQ(snk->state, caf::flow::observer_state::subscribed);
        CHECK_EQ(snk->buf, ivec({42, 42, 42}));
        ks.dispose();
        ctx->run();
        CHECK(snk->sub.as_disposable().disposed());
        CHECK_EQ(snk->state, caf::flow::observer_state::completed);
        CHECK_EQ(snk->buf, ivec({42, 42, 42}));
      }
    }
    WHEN("inserting a killswitch via inject_killswitch_t") {
      THEN("the killswitch allows us to cancel the subscription mid-flow") {
        auto ks = caf::disposable{};
        auto snk = caf::flow::make_passive_observer<int>();
        ctx //
          ->make_observable()
          .repeat(42)
          .compose(internal::inject_killswitch_t{&ks})
          .subscribe(snk->as_observer());
        snk->sub.request(3);
        ctx->run();
        CHECK_EQ(snk->state, caf::flow::observer_state::subscribed);
        CHECK_EQ(snk->buf, ivec({42, 42, 42}));
        ks.dispose();
        ctx->run();
        CHECK(snk->sub.as_disposable().disposed());
        CHECK_EQ(snk->state, caf::flow::observer_state::completed);
        CHECK_EQ(snk->buf, ivec({42, 42, 42}));
      }
    }
  }
}

END_FIXTURE_SCOPE()
