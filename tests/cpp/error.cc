#define SUITE error

#include "broker/error.hh"

#include "test.hh"

#include <string>

using namespace broker;
using namespace std::string_literals;

TEST(ec is convertible to and from string) {
  CHECK_EQUAL(to_string(ec::unspecified), "unspecified"s);
  CHECK_EQUAL(to_string(ec::peer_incompatible), "peer_incompatible"s);
  CHECK_EQUAL(to_string(ec::peer_invalid), "peer_invalid"s);
  CHECK_EQUAL(to_string(ec::peer_unavailable), "peer_unavailable"s);
  CHECK_EQUAL(to_string(ec::peer_timeout), "peer_timeout"s);
  CHECK_EQUAL(to_string(ec::master_exists), "master_exists"s);
  CHECK_EQUAL(to_string(ec::no_such_master), "no_such_master"s);
  CHECK_EQUAL(to_string(ec::no_such_key), "no_such_key"s);
  CHECK_EQUAL(to_string(ec::request_timeout), "request_timeout"s);
  CHECK_EQUAL(to_string(ec::type_clash), "type_clash"s);
  CHECK_EQUAL(to_string(ec::invalid_data), "invalid_data"s);
  CHECK_EQUAL(to_string(ec::backend_failure), "backend_failure"s);
  CHECK_EQUAL(to_string(ec::stale_data), "stale_data"s);
  CHECK_EQUAL(to_string(ec::cannot_open_file), "cannot_open_file"s);
  CHECK_EQUAL(to_string(ec::cannot_write_file), "cannot_write_file"s);
  CHECK_EQUAL(to_string(ec::invalid_topic_key), "invalid_topic_key"s);
  CHECK_EQUAL(to_string(ec::end_of_file), "end_of_file"s);
  CHECK_EQUAL(to_string(ec::invalid_tag), "invalid_tag"s);
  CHECK_EQUAL(from_string<ec>("unspecified"), ec::unspecified);
  CHECK_EQUAL(from_string<ec>("peer_incompatible"), ec::peer_incompatible);
  CHECK_EQUAL(from_string<ec>("peer_invalid"), ec::peer_invalid);
  CHECK_EQUAL(from_string<ec>("peer_unavailable"), ec::peer_unavailable);
  CHECK_EQUAL(from_string<ec>("peer_timeout"), ec::peer_timeout);
  CHECK_EQUAL(from_string<ec>("master_exists"), ec::master_exists);
  CHECK_EQUAL(from_string<ec>("no_such_master"), ec::no_such_master);
  CHECK_EQUAL(from_string<ec>("no_such_key"), ec::no_such_key);
  CHECK_EQUAL(from_string<ec>("request_timeout"), ec::request_timeout);
  CHECK_EQUAL(from_string<ec>("type_clash"), ec::type_clash);
  CHECK_EQUAL(from_string<ec>("invalid_data"), ec::invalid_data);
  CHECK_EQUAL(from_string<ec>("backend_failure"), ec::backend_failure);
  CHECK_EQUAL(from_string<ec>("stale_data"), ec::stale_data);
  CHECK_EQUAL(from_string<ec>("cannot_open_file"), ec::cannot_open_file);
  CHECK_EQUAL(from_string<ec>("cannot_write_file"), ec::cannot_write_file);
  CHECK_EQUAL(from_string<ec>("invalid_topic_key"), ec::invalid_topic_key);
  CHECK_EQUAL(from_string<ec>("end_of_file"), ec::end_of_file);
  CHECK_EQUAL(from_string<ec>("invalid_tag"), ec::invalid_tag);
  CHECK_EQUAL(from_string<ec>("none"), nil);
  CHECK_EQUAL(from_string<ec>("foo"), nil);
}
