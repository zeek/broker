#pragma once

#include "broker/configuration.hh"

#include <caf/expected.hpp>
#include <caf/net/ssl/context.hpp>

namespace broker::internal {

/// Creates an SSL context from the given options.
/// @param options The options to create the SSL context from.
/// @returns The created SSL context if `options` is not `nullptr` and contains
///          valid options. If `options` is `nullptr`, returns an `expected`
///          holding a default-constructed error. Otherwise, returns an
///          `expected` holding the error that occurred while creating the SSL
///          context.
caf::expected<caf::net::ssl::context>
ssl_context_from_options(const openssl_options_ptr& options);

} // namespace broker::internal
