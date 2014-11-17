#ifndef BROKER_BROKER_H
#define BROKER_BROKER_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// TODO: automate how version levels are set here.
#define BROKER_VERSION_MAJOR 0
#define BROKER_VERSION_MINOR 1
#define BROKER_VERSION_PATCH 0

/**
 * The version of the broker messaging protocol.  Endpoints can only
 * exchange messages if they use the same version.
 */
const int BROKER_PROTOCOL_VERSION = 0;

/**
 * Initialize the broker library.  This should be called once before using
 * anything else that's provided by the library.
 * @param flags tune behavior of the library.  No flags exist yet.
 * @return 0 if library is initialized, else an error code that can
 *         be supplied to broker_strerror().
 */
int broker_init(int flags);

/**
 * Shutdown the broker library.  No functionality provided by the library
 * is guaranteed to work after the call, not even destructors of broker-related
 * objects on the stack, so be careful of that.  Note that it's not required
 * to call this at all if the application just intends to exit.
 */
void broker_done();

/**
 * @return a textual representation of a broker error code.
 */
const char* broker_strerror(int broker_errno);

/**
 * Reentrant version of broker::strerror().
 * @return a return value from ::strerror_r().
 */
int broker_strerror_r(int broker_errno, char* buf, size_t buflen);

// TODO: add wrappers for more of the C++ API

#ifdef __cplusplus
} // extern "C"
#endif

#endif // BROKER_BROKER_H
