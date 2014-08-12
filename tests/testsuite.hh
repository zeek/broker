#include <iostream>
#include <atomic>

namespace broker { namespace testsuite {

static std::atomic<size_t> num_errors(0);

int result()
	{ return num_errors == 0 ? 0 : -1; }

void failure(const char* filename, size_t line, const char* msg)
	{
	std::cerr << "TEST FAIL in " << filename << ":" << line << std::endl
	          << "\texpression: (" << msg << ")" << std::endl;
	++num_errors;
	}
}
}

#define BROKER_TEST(expression) \
	(expression) ? ((void)0) \
	             : broker::testsuite::failure(__FILE__, __LINE__, #expression)

#define BROKER_TEST_RESULT() (broker::testsuite::result())
