# This test sets the default values and then runs a few put and get operations
# to see that the database is operational.
#
# @TEST-GROUP: store
#
# @TEST-EXEC: btest-bg-run prog "btest-sqlite-driver --program=../prog.json > out.txt"
# @TEST-EXEC: btest-bg-wait 30
# @TEST-EXEC: btest-diff prog/out.txt

# Should report "2" for the "synchronous" pragma and "delete" for the
# "journal_mode" pragma, i.e., the default settings.
@TEST-START-FILE prog.json
{
    "config": {
        "file-path": "test.db",
        "options": {}
    },
    "commands": [
        ["exec-pragma", ["synchronous"]],
        ["exec-pragma", ["journal_mode"]],
        ["put", ["key1", "value1"]],
        ["put", ["key2", "value2"]],
        ["put", ["key3", "value3"]],
        ["get", ["key1"]]
    ]
}
@TEST-END-FILE
