#include "broker/status.hh"

namespace broker {

const char* to_string(sc code) {
  switch (code) {
    default:
      BROKER_ASSERT(!"missing to_string implementation");
      return "<unknown>";
    case sc::unspecified:
      return "<unknown>";
    case sc::peer_added:
      return "peer_added";
    case sc::peer_removed:
      return "peer_removed";
    case sc::peer_incompatible:
      return "peer_incompatible";
    case sc::peer_invalid:
      return "peer_invalid";
    case sc::peer_unavailable:
      return "peer_unavailable";
    case sc::peer_timeout:
      return "peer_timeout";
    case sc::peer_lost:
      return "peer_lost";
    case sc::peer_recovered:
      return "peer_recovered";
    case sc::master_exists:
      return "master_exists";
    case sc::no_such_master:
      return "no_such_master";
    case sc::no_such_key:
      return "no_such_key";
    case sc::request_timeout:
      return "request_timeout";
    case sc::type_clash:
      return "type_clash";
    case sc::invalid_data:
      return "invalid_data";
    case sc::backend_failure:
      return "backend_failure";
  }
}

status::status(sc code) : error_{code} {
}

bool status::error() const {
  if (error_ == caf::none)
    return false;
  BROKER_ASSERT(error_.category() == caf::atom("broker"));
  switch (static_cast<sc>(error_.code())) {
    default:
      return true;
    case sc::peer_added:
    case sc::peer_removed:
    case sc::peer_recovered:
      return false;
  }
}

sc status::code() const {
  if (error_ == caf::none)
    return sc::unspecified;
  BROKER_ASSERT(error_.category() == caf::atom("broker"));
  return static_cast<sc>(error_.code());
}

const std::string* status::message() const {
  if (error_ == caf::none)
    return nullptr;
  BROKER_ASSERT(error_.category() == caf::atom("broker"));
  auto& ctx = error_.context();
  switch (static_cast<sc>(error_.code())) {
    default:
      return nullptr;
    case sc::unspecified:
      return ctx.empty() ? nullptr : &ctx.get_as<std::string>(0);
    case sc::peer_added:
    case sc::peer_removed:
    case sc::peer_incompatible:
    case sc::peer_invalid:
    case sc::peer_unavailable:
    case sc::peer_timeout:
    case sc::peer_lost:
    case sc::peer_recovered:
      return &ctx.get_as<std::string>(1);
    case sc::request_timeout:
      return &ctx.get_as<std::string>(0);
  }
}

bool operator==(const status& x, const status& y) {
  return x.error_ == y.error_;
}

bool operator==(const status& x, sc y) {
  return x.code() == y;
}

bool operator==(sc x, const status& y) {
  return y == x;
}

std::string to_string(const status& s) {
  if (s.error_ == caf::none)
    return "";
  BROKER_ASSERT(s.error_.category() == caf::atom("broker"));
  auto code = static_cast<sc>(s.error_.code());
  // TODO: include context and message.
  return to_string(code);
}

status status::make(caf::error e) {
  if (e != caf::none && e.category() == caf::atom("broker"))
    // All unhandled cases are never shipped around as caf::error because
    // Broker generates them directly.
    switch (static_cast<sc>(e.code())) {
      default:
        return make<sc::unspecified>();
      case sc::unspecified:
        if (e.context().empty() || e.context().match_elements<std::string>())
          return status{std::move(e)};
        break;
      case sc::master_exists:
      case sc::no_such_master:
      case sc::no_such_key:
      case sc::type_clash:
      case sc::invalid_data:
      case sc::backend_failure:
        if (e.context().empty())
          return status{std::move(e)};
        break;
    }
  return make<sc::unspecified>();
}

status::status(caf::error e) : error_{std::move(e)} {
}

status make_status(caf::error e) {
  return status::make(std::move(e));
}

caf::error make_error(const status& s) {
  return s.error_;
}

} // namespace broker
