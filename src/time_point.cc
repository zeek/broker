#include "broker/time_point.hh"
#include <chrono>

broker::time_point broker::time_point::now()
	{
	using namespace std::chrono;
	auto d = system_clock::now().time_since_epoch();
	return time_point{duration_cast<duration<double>>(d).count()};
	}
