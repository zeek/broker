#include "broker/internal/ssl_context_from_options.hh"

#include "broker/configuration.hh"
#include "broker/logger.hh"

#include <caf/expected.hpp>
#include <caf/net/ssl/context.hpp>
#include <caf/net/ssl/verify.hpp>

namespace broker::internal {

caf::expected<caf::net::ssl::context>
ssl_context_from_options(const openssl_options_ptr& options) {
  caf::expected<caf::net::ssl::context> result{caf::error{}};
  if (options) {
    auto pem = caf::net::ssl::format::pem;
    auto ctx = caf::net::ssl::context::make(caf::net::ssl::tls::v1_2);
    auto ok = true;
    if (!options->certificate.empty()) {
      ok = ctx->use_certificate_chain_file(options->certificate);
    }
    if (ok && !options->key.empty()) {
      ok = ctx->use_private_key_file(options->key, pem);
    }
    if (ok && !options->cafile.empty()) {
      ok = ctx->add_verify_path(options->cafile);
    }
    if (ok && !options->cafile.empty()) {
      ok = ctx->load_verify_file(options->cafile);
    }
    if (ok && !options->capath.empty()) {
      ok = ctx->add_verify_path(options->capath);
    }
    if (ok) {
      ctx->password(options->passphrase);
      // If no certificate is provided, disable peer verification to allow
      // connections without certificates (e.g., for testing).
      if (options->certificate.empty() || options->key.empty()) {
        ctx->verify_mode(caf::net::ssl::verify::none);
      }
      result = std::move(ctx);
    } else {
      result = ctx->last_error();
    }
  }
  return result;
}

} // namespace broker::internal
