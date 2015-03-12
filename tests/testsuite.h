#include <stdio.h>

static size_t broker_test_num_errors = 0;

int broker_test_result()
	{ return broker_test_num_errors == 0 ? 0 : -1; }

void broker_test_failure(const char* filename, size_t line, const char* expr)
	{
	printf("TEST FAIL in %s:%zu\n\texpression: (%s)\n", filename, line, expr);
	++broker_test_num_errors;
	}

#define BROKER_TEST(expression) \
	(expression) ? ((void)0) \
	             : broker_test_failure(__FILE__, __LINE__, #expression)

#define BROKER_TEST_RESULT() (broker_test_result())
