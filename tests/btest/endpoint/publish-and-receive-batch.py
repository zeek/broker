# @TEST-GROUP: endpoint
#
# @TEST-PORT: BROKER_PORT
#
# @TEST-EXEC: btest-bg-run publisher "btest-publish-and-receive publisher 256 batch"
# @TEST-EXEC: btest-publish-and-receive receiver 256 > receiver.txt
# @TEST-EXEC: btest-bg-wait -k 10
# @TEST-EXEC: btest-diff receiver.txt
#
# Note: this is also a regression test for GH196. The default capacity of the
#       buffer in the publisher is 128, so we need to send more than that to
#       trigger the bug.
