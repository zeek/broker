# @TEST-GROUP: endpoint
#
# @TEST-PORT: BROKER_PORT
#
# @TEST-EXEC: btest-bg-run forwarder "btest-forwarding forwarder > forwarder.txt"
# @TEST-EXEC: btest-bg-run publisher "btest-forwarding publisher 10 > publisher.txt"
# @TEST-EXEC: btest-forwarding receiver 10 > receiver.txt
# @TEST-EXEC: btest-bg-wait -k 10
# @TEST-EXEC: btest-diff receiver.txt
