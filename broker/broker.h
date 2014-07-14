#ifndef BROKER_BROKER_H
#define BROKER_BROKER_H

#ifdef __cplusplus
extern "C" {
#endif

// TODO: automate how version levels are set here.
#define BROKER_VERSION_MAJOR 0
#define BROKER_VERSION_MINOR 1
#define BROKER_VERSION_PATCH 0

int broker_init(int flags);

void broker_done();

const char* broker_strerror(int arg_errno);

// TODO: add wrappers for more of the C++ API

#ifdef __cplusplus
} // extern "C"
#endif

#endif // BROKER_BROKER_H
