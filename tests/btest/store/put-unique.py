# @TEST-GROUP: store
#
# @TEST-PORT: MASTER_PORT
# @TEST-PORT: CL_0_PORT
# @TEST-PORT: CL_1_PORT
# @TEST-PORT: CL_2_PORT
# @TEST-PORT: CL_3_PORT
# @TEST-PORT: CL_4_PORT
# @TEST-PORT: CL_5_PORT
# @TEST-PORT: CL_6_PORT
# @TEST-PORT: CL_7_PORT
#
# @TEST-EXEC: btest-bg-run master "btest-put-unique --config-file=../broker.cfg -m master -n 8 > out.txt"
# @TEST-EXEC: btest-bg-run clone_0 "btest-put-unique --config-file=../broker.cfg -m clone -n 8 -c 0 > out.txt"
# @TEST-EXEC: btest-bg-run clone_1 "btest-put-unique --config-file=../broker.cfg -m clone -n 8 -c 1 > out.txt"
# @TEST-EXEC: btest-bg-run clone_2 "btest-put-unique --config-file=../broker.cfg -m clone -n 8 -c 2 > out.txt"
# @TEST-EXEC: btest-bg-run clone_3 "btest-put-unique --config-file=../broker.cfg -m clone -n 8 -c 3 > out.txt"
# @TEST-EXEC: btest-bg-run clone_4 "btest-put-unique --config-file=../broker.cfg -m clone -n 8 -c 4 > out.txt"
# @TEST-EXEC: btest-bg-run clone_5 "btest-put-unique --config-file=../broker.cfg -m clone -n 8 -c 5 > out.txt"
# @TEST-EXEC: btest-bg-run clone_6 "btest-put-unique --config-file=../broker.cfg -m clone -n 8 -c 6 > out.txt"
# @TEST-EXEC: btest-bg-run clone_7 "btest-put-unique --config-file=../broker.cfg -m clone -n 8 -c 7 > out.txt"
#
# @TEST-EXEC: btest-bg-wait 30
# @TEST-EXEC: btest-bg-run total "cat ../clone_*/out.txt | sort > out.txt"
# @TEST-EXEC: btest-diff total/out.txt

@TEST-START-FILE broker.cfg

broker {
  disable-forwarding = true
}
caf {
  logger {
    file {
      path = 'broker.log'
      verbosity = 'debug'
    }
  }
}
topics = ["/test"]

@TEST-END-FILE
