#include "main.hh"

#include "broker/configuration.hh"

#include <benchmark/benchmark.h>

#include <caf/init_global_meta_objects.hpp>

#include <cstdlib>

int main(int argc, char** argv) {
  caf::init_global_meta_objects<caf::id_block::micro_benchmarks>();
  broker::configuration::init_global_state();
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
    return EXIT_FAILURE;
  } else {
    benchmark::RunSpecifiedBenchmarks();
    return EXIT_SUCCESS;
  }
}
