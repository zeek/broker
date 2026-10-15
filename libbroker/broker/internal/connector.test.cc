#include "broker/internal/connector.hh"

#include "broker/broker-test.test.hh"

#include "broker/configuration.hh"

#include <openssl/evp.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>

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

constexpr std::string_view custom_key =
  "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
  "202122232425262728292a2b2c2d2e2f";

/// Creates an SSL context the way Broker does without certificates.
ctx_ptr make_psk_ctx(std::string_view key = {}) {
  auto cfg = std::make_shared<openssl_options>();
  cfg->psk_key = std::string{key};
  return internal::ssl_context_from_cfg(cfg);
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

#if OPENSSL_VERSION_NUMBER >= 0x30000000L

/// Creates an SSL context for a server that knows no PSK, but presents a
/// self-signed certificate.
ctx_ptr make_rogue_ctx() {
  auto ctx = caf::net::openssl::make_ctx(TLS_method());
  auto key = EVP_PKEY_Q_keygen(nullptr, nullptr, "EC", "P-256");
  auto cert = X509_new();
  X509_set_version(cert, 2);
  ASN1_INTEGER_set(X509_get_serialNumber(cert), 1);
  X509_gmtime_adj(X509_getm_notBefore(cert), 0);
  X509_gmtime_adj(X509_getm_notAfter(cert), 3600);
  X509_set_pubkey(cert, key);
  auto name = X509_get_subject_name(cert);
  X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
                             reinterpret_cast<const unsigned char*>("rogue"),
                             -1, -1, 0);
  X509_set_issuer_name(cert, name);
  X509_sign(cert, key, EVP_sha256());
  SSL_CTX_use_certificate(ctx.get(), cert);
  SSL_CTX_use_PrivateKey(ctx.get(), key);
  X509_free(cert);
  EVP_PKEY_free(key);
  return ctx;
}

#endif

struct handshake_result {
  bool ok = false;
  std::string version;
  std::string cipher;
  bool psk = false;
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
    result.psk = SSL_session_reused(client) == 1;
  }
  SSL_free(server);
  SSL_free(client);
  close(fds[0]);
  close(fds[1]);
#endif
  return result;
}

} // namespace

#ifndef BROKER_WINDOWS

TEST(peers with the default PSK use TLS 1.3) {
  auto res = handshake(make_psk_ctx(), make_psk_ctx());
  CHECK(res.ok);
  CHECK_EQ(res.version, "TLSv1.3");
  CHECK_EQ(res.cipher, "TLS_AES_256_GCM_SHA384");
  CHECK(res.psk);
}

TEST(peers with a custom PSK use TLS 1.3) {
  auto res = handshake(make_psk_ctx(custom_key), make_psk_ctx(custom_key));
  CHECK(res.ok);
  CHECK_EQ(res.version, "TLSv1.3");
  CHECK_EQ(res.cipher, "TLS_AES_256_GCM_SHA384");
  CHECK(res.psk);
}

TEST(peers with different PSKs fail the handshake) {
  CHECK(!handshake(make_psk_ctx(custom_key), make_psk_ctx()).ok);
  CHECK(!handshake(make_psk_ctx(), make_psk_ctx(custom_key)).ok);
}

TEST(the default PSK accepts anonymous TLS 1.2 from older peers) {
  auto res = handshake(make_psk_ctx(), make_legacy_ctx());
  CHECK(res.ok);
  CHECK_EQ(res.version, "TLSv1.2");
  CHECK_EQ(res.cipher, "AECDH-AES256-SHA");
  CHECK(!res.psk);
}

TEST(the default PSK falls back to anonymous TLS 1.2 with older peers) {
  auto res = handshake(make_legacy_ctx(), make_psk_ctx());
  CHECK(res.ok);
  CHECK_EQ(res.version, "TLSv1.2");
  CHECK_EQ(res.cipher, "AECDH-AES256-SHA");
  CHECK(!res.psk);
}

TEST(a custom PSK rejects older peers) {
  CHECK(!handshake(make_psk_ctx(custom_key), make_legacy_ctx()).ok);
  CHECK(!handshake(make_legacy_ctx(), make_psk_ctx(custom_key)).ok);
}

#  if OPENSSL_VERSION_NUMBER >= 0x30000000L

TEST(clients reject servers that present a certificate instead of the PSK) {
  CHECK(!handshake(make_rogue_ctx(), make_psk_ctx()).ok);
  CHECK(!handshake(make_rogue_ctx(), make_psk_ctx(custom_key)).ok);
}

#  endif

#endif
