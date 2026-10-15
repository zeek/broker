#include "broker/internal/connector.hh"

#include "broker/broker-test.test.hh"

#include "broker/configuration.hh"

#include <openssl/ssl.h>

#include <memory>
#include <string>
#include <thread>

#ifndef BROKER_WINDOWS
#  include <sys/socket.h>
#  include <unistd.h>
#endif

using namespace broker;

namespace {

using ctx_ptr = caf::net::openssl::ctx_ptr;

/// Creates an SSL context the way Broker does without certificates.
ctx_ptr make_anon_ctx() {
  return internal::ssl_context_from_cfg(std::make_shared<openssl_options>());
}

/// Creates an SSL context like older Broker versions without certificates.
ctx_ptr make_legacy_ctx() {
  auto ctx = caf::net::openssl::make_ctx(TLS_method());
  SSL_CTX_set_verify(ctx.get(), SSL_VERIFY_NONE, nullptr);
  SSL_CTX_set1_groups_list(ctx.get(), "P-384");
  SSL_CTX_set_cipher_list(ctx.get(), "AECDH-AES256-SHA@SECLEVEL=0");
  SSL_CTX_set_min_proto_version(ctx.get(), TLS1_2_VERSION);
  return ctx;
}

struct handshake_result {
  bool ok = false;
  std::string version;
  std::string cipher;
  bool server_cert = false;
};

/// Runs a TLS handshake between a server and a client context.
handshake_result handshake(const ctx_ptr& server_ctx,
                           const ctx_ptr& client_ctx) {
  handshake_result result;
#ifndef BROKER_WINDOWS
  int fds[2];
  if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
    FAIL("socketpair failed");
  }
  auto server = SSL_new(server_ctx.get());
  auto client = SSL_new(client_ctx.get());
  SSL_set_fd(server, fds[0]);
  SSL_set_fd(client, fds[1]);
  // On failure, shut down the socket to unblock the other side.
  bool server_ok = false;
  auto server_thread = std::thread{[&] {
    server_ok = SSL_accept(server) == 1;
    if (!server_ok)
      shutdown(fds[0], SHUT_RDWR);
  }};
  auto client_ok = SSL_connect(client) == 1;
  if (!client_ok)
    shutdown(fds[1], SHUT_RDWR);
  server_thread.join();
  result.ok = server_ok && client_ok;
  if (result.ok) {
    result.version = SSL_get_version(client);
    result.cipher = SSL_get_cipher_name(client);
    result.server_cert = SSL_get0_peer_certificate(client) != nullptr;
  }
  SSL_free(server);
  SSL_free(client);
  close(fds[0]);
  close(fds[1]);
#endif
  return result;
}

} // namespace

#if !defined(BROKER_WINDOWS) && OPENSSL_VERSION_NUMBER >= 0x30000000L

TEST(peers without certificates use TLS 1.3 with a self signed certificate) {
  auto res = handshake(make_anon_ctx(), make_anon_ctx());
  CHECK(res.ok);
  CHECK_EQ(res.version, "TLSv1.3");
  CHECK(res.server_cert);
}

TEST(older peers connect via TLS 1.3 using the self signed certificate) {
  auto res = handshake(make_anon_ctx(), make_legacy_ctx());
  CHECK(res.ok);
  CHECK_EQ(res.version, "TLSv1.3");
  CHECK(res.server_cert);
}

TEST(connecting to older peers falls back to anonymous TLS 1.2) {
  auto res = handshake(make_legacy_ctx(), make_anon_ctx());
  CHECK(res.ok);
  CHECK_EQ(res.version, "TLSv1.2");
  CHECK_EQ(res.cipher, "AECDH-AES256-SHA");
  CHECK(!res.server_cert);
}

#endif
