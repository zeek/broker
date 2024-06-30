# @TEST-GROUP: endpoint
#
# @TEST-PORT: BROKER_PORT
#
# @TEST-EXEC: btest-bg-run node1 "btest-publish-and-receive publisher"
# @TEST-EXEC: btest-publish-and-receive receiver > out.txt
# @TEST-EXEC: btest-bg-wait -k 10
# @TEST-EXEC: btest-diff out.txt
