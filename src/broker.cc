#include "broker/broker.hh"

#include <cppa/cppa.hpp>

#include <cstdio>

int broker::init(int flags)
	{
	// TODO: cppa announcements
	return 0;
	}

int broker_init(int flags)
	{
	return broker::init(flags);
	}

void broker::done()
	{
	cppa::shutdown();
	}

void broker_done()
	{
	return broker::done();
	}

const char* broker::strerror(int arg_errno)
	{
	switch ( arg_errno ) {
	default:
		return strerror(arg_errno);
	};
	}

const char* broker_strerror(int arg_errno)
	{
	return broker::strerror(arg_errno);
	}
