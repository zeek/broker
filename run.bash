#!/bin/bash

lst="\"localhost:4242\""
for i in {1..5} ; do
  p=$(echo 4242 + $i | bc)
  echo "run -p $p --peers=[$lst]"
  { ./build/release/bin/broker-benchmark --caf.scheduler.max-threads=2 --verbose -r 1000 -p $p --peers="[$lst]" & }
  lst="\"localhost:$p\",$lst"
done
