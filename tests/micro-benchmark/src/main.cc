#include "broker/configuration.hh"

#include <benchmark/benchmark.h>

#include <cstdlib>

int main(int argc, char** argv) {
  broker::configuration::init_global_state();
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
    return EXIT_FAILURE;
  } else {
    benchmark::RunSpecifiedBenchmarks();
    return EXIT_SUCCESS;
  }
}
