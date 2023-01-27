# This test checks the journal mode settings.
#
# @TEST-GROUP: store
#
# @TEST-EXEC: btest-bg-run prog1 "btest-sqlite-driver --program=../prog1.json > out.txt"
# @TEST-EXEC: btest-bg-run prog2 "btest-sqlite-driver --program=../prog2.json > out.txt"
# @TEST-EXEC: btest-bg-wait 30
# @TEST-EXEC: awk 'FNR==1 {print "==> ", FILENAME}{print}' prog*/out.txt > all.txt
# @TEST-EXEC: btest-diff all.txt

# Should report "delete" when running the pragma.
@TEST-START-FILE prog1.json
{
    "config": {
        "file-path": "test.db",
        "options": {
            "journal_mode": "Broker::SQLITE_JOURNAL_MODE_DELETE"
        }
    },
    "commands": [
        ["exec-pragma", ["journal_mode"]],
        ["put", ["key1", "value1"]],
        ["get", ["key1"]]
    ]
}
@TEST-END-FILE

# Should report "wal" when running the pragma.
@TEST-START-FILE prog2.json
{
    "config": {
        "file-path": "test.db",
        "options": {
            "synchronous": "Broker::SQLITE_SYNCHRONOUS_NORMAL",
            "journal_mode": "Broker::SQLITE_JOURNAL_MODE_WAL"
        }
    },
    "commands": [
        ["exec-pragma", ["journal_mode"]],
        ["put", ["key1", "value1"]],
        ["get", ["key1"]]
    ]
}
@TEST-END-FILE
