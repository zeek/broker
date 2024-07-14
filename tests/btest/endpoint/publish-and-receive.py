# @TEST-GROUP: endpoint
#
# @TEST-PORT: BROKER_PORT
#
# @TEST-EXEC: btest-bg-run publisher "btest-publish-and-receive publisher 10 single"
# @TEST-EXEC: btest-publish-and-receive receiver 10 > receiver.txt
# @TEST-EXEC: btest-bg-wait -k 10
# @TEST-EXEC: btest-diff receiver.txt
