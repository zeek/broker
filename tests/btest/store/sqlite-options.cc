#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest.h>

#include "broker/data.hh"
#include "broker/detail/filesystem.hh"
#include "broker/detail/sqlite_backend.hh"

#include <iostream>
#include <ostream>
#include <string>
#include <vector>

using namespace broker;
using namespace std::literals;

// -- program options ----------------------------------------------------------

namespace {

struct sqlite_fixture {
  sqlite_fixture()
    : path{detail::make_temp_file_name()}, opts{{"path", path}} {}

  ~sqlite_fixture() {
    detail::remove_all(path);
    detail::remove_all(path + "-wal"); // Created when journal_mode=wal.
    detail::remove_all(path + "-shm"); // Created when journal_mode=wal.
  }
  std::string path;
  backend_options opts;
  std::vector<std::string> msgs;

  std::unique_ptr<detail::sqlite_backend>
  make_backend(const backend_options& opts) {
    return std::make_unique<detail::sqlite_backend>(opts);
  }
};

} // namespace

TEST_CASE("backend sqlite") {
  struct sqlite_fixture fixture;

  SUBCASE("defaults") {
    auto backend = std::make_unique<detail::sqlite_backend>(fixture.opts);

    CHECK(!backend->init_failed());

    backend->exec_pragma("synchronous", "", &fixture.msgs);
    backend->exec_pragma("journal_mode", "", &fixture.msgs);

    CHECK(fixture.msgs.size() == 2);
    CHECK(fixture.msgs[0] == "2"); // 2 is FULL
    CHECK(fixture.msgs[1] == "delete");
  }

  SUBCASE("missing path") {
    auto backend = std::make_unique<detail::sqlite_backend>(backend_options{});
    CHECK(backend->init_failed());
  }

  SUBCASE("journal mode wal") {
    fixture.opts["journal_mode"] =
      enum_value{"Broker::SQLITE_JOURNAL_MODE_WAL"};
    fixture.opts["synchronous"] =
      enum_value{"Broker::SQLITE_SYNCHRONOUS_NORMAL"};
    auto backend = std::make_unique<detail::sqlite_backend>(fixture.opts);

    CHECK(!backend->init_failed());

    backend->exec_pragma("synchronous", "", &fixture.msgs);
    backend->exec_pragma("journal_mode", "", &fixture.msgs);

    CHECK(fixture.msgs.size() == 2);
    CHECK(fixture.msgs[0] == "1"); // 1 is normal
    CHECK(fixture.msgs[1] == "wal");
  }

  SUBCASE("journal mode delete") {
    fixture.opts["journal_mode"] =
      enum_value{"Broker::SQLITE_JOURNAL_MODE_DELETE"};
    auto backend = std::make_unique<detail::sqlite_backend>(fixture.opts);

    CHECK(!backend->init_failed());

    backend->exec_pragma("journal_mode", "", &fixture.msgs);

    CHECK(fixture.msgs.size() == 1);
    CHECK(fixture.msgs[0] == "delete");
  }

  SUBCASE("bad journal mode - wrong prefix") {
    // Wrong prefix
    fixture.opts["journal_mode"] =
      enum_value{"BBBBroker::SQLITE_JOURNAL_MODE_WAL"};
    auto backend = std::make_unique<detail::sqlite_backend>(fixture.opts);
    CHECK(backend->init_failed());
  }

  SUBCASE("bad journal mode - wrong type") {
    struct sqlite_fixture fixture;
    fixture.opts["journal_mode"] =
      std::string{"Broker::SQLITE_JOURNAL_MODE_WAL"};
    auto backend = std::make_unique<detail::sqlite_backend>(fixture.opts);
    CHECK(backend->init_failed());
  }

  SUBCASE("bad journal mode - wrong value") {
    struct sqlite_fixture fixture;
    fixture.opts["journal_mode"] =
      std::string{"Broker::SQLITE_JOURNAL_MODE_HUH"};
    auto backend = std::make_unique<detail::sqlite_backend>(fixture.opts);
    CHECK(backend->init_failed());
  }

  SUBCASE("valid synchronous") {
    std::vector<enum_value> values = {
      enum_value{"Broker::SQLITE_SYNCHRONOUS_OFF"},
      enum_value{"Broker::SQLITE_SYNCHRONOUS_NORMAL"},
      enum_value{"Broker::SQLITE_SYNCHRONOUS_FULL"},
      enum_value{"Broker::SQLITE_SYNCHRONOUS_EXTRA"},
    };

    int i = 0;
    for (const auto& v : values) {
      struct sqlite_fixture fixture;
      fixture.opts["synchronous"] = v;
      auto backend = std::make_unique<detail::sqlite_backend>(fixture.opts);
      CHECK(!backend->init_failed());

      backend->exec_pragma("synchronous", "", &fixture.msgs);
      CHECK(fixture.msgs[0] == std::to_string(i));
      ++i;
    }
  }

  SUBCASE("bad synchronous - wrong prefix") {
    fixture.opts["synchronous"] =
      enum_value{"BBBBroker::SQLITE_SYNCHRONOUS_NORMAL"};
    auto backend = std::make_unique<detail::sqlite_backend>(fixture.opts);
    CHECK(backend->init_failed());
  }

  SUBCASE("bad synchronous - wrong type") {
    fixture.opts["synchronous"] =
      std::string("Broker::SQLITE_SYNCHRONOUS_NORMAL");
    auto backend = std::make_unique<detail::sqlite_backend>(fixture.opts);
    CHECK(backend->init_failed());
  }

  SUBCASE("bad synchronous - wrong value") {
    fixture.opts["synchronous"] = enum_value{"Broker::SQLITE_SYNCHRONOUS_HUH"};
    auto backend = std::make_unique<detail::sqlite_backend>(fixture.opts);
    CHECK(backend->init_failed());
  }

  SUBCASE("valid integrity_check") {
    fixture.opts["integrity_check"] = boolean{true};
    auto backend = std::make_unique<detail::sqlite_backend>(fixture.opts);
    CHECK(!backend->init_failed());
  }

  SUBCASE("bad integrity_check") {
    fixture.opts["integrity_check"] = std::string("true");
    auto backend = std::make_unique<detail::sqlite_backend>(fixture.opts);
    CHECK(backend->init_failed());
  }

  SUBCASE("valid failure_mode - fail") {
    fixture.opts["failure_mode"] =
      enum_value{"Broker::SQLITE_FAILURE_MODE_FAIL"};
    auto backend = std::make_unique<detail::sqlite_backend>(fixture.opts);
    CHECK(!backend->init_failed());
  }

  SUBCASE("valid failure_mode - delete") {
    fixture.opts["failure_mode"] =
      enum_value{"Broker::SQLITE_FAILURE_MODE_DELETE"};
    auto backend = std::make_unique<detail::sqlite_backend>(fixture.opts);
    CHECK(!backend->init_failed());
  }

  SUBCASE("valid failure_mode - wrong type") {
    fixture.opts["failure_mode"] =
      std::string{"Broker::SQLITE_FAILURE_MODE_DELETE"};
    auto backend = std::make_unique<detail::sqlite_backend>(fixture.opts);
    CHECK(backend->init_failed());
  }

  SUBCASE("failure_mode - delete on corruption") {
    // Insert k -> v into the database.
    auto backend = std::make_unique<detail::sqlite_backend>(fixture.opts);
    CHECK(!backend->init_failed());
    backend->put(data{"k"}, data{"v"}, std::optional<timestamp>());
    auto r1 = backend->get(data{"k"});
    CHECK(r1);
    auto v = get_if<std::string>(r1.value());
    CHECK(v);
    CHECK(*v == "v");
    backend.reset();

    // Corrupt the database, try to reopen it in failure_mode
    {
      std::ofstream dbfile;
      dbfile.open(fixture.path, std::ios::binary);
      CHECK(dbfile.is_open());
      std::string garbage{"AAAA"};
      dbfile.seekp(32);
      dbfile.write(garbage.c_str(), garbage.size());
      dbfile.flush();
      dbfile.close();
    }

    // Reopen fails because the DB is now corrupted.
    backend = std::make_unique<detail::sqlite_backend>(fixture.opts);
    CHECK(backend->init_failed());
    backend.reset();

    // Reopen works in DELETE mode, but the data will be gone.
    fixture.opts["failure_mode"] =
      enum_value{"Broker::SQLITE_FAILURE_MODE_DELETE"};
    backend = std::make_unique<detail::sqlite_backend>(fixture.opts);
    CHECK(!backend->init_failed());
    auto r2 = backend->get(data{"k"});
    CHECK(!r2);
  }
}
