# This test checks the failure modes by setting a specific failure mode and then
# trying to open a corrupt database file.
#
# @TEST-GROUP: store
#
# @TEST-EXEC: btest-bg-run writer1 "btest-sqlite-driver --program=../writer.json > out.txt"
# @TEST-EXEC: btest-bg-run writer2 "btest-sqlite-driver --program=../writer.json > out.txt"
# @TEST-EXEC: btest-bg-wait 30
# @TEST-EXEC: python3 destroydb.py writer1/test.db
# @TEST-EXEC: python3 destroydb.py writer2/test.db
# @TEST-EXEC: btest-bg-run prog1 "btest-sqlite-driver --program=../prog1.json > out.txt"
# @TEST-EXEC: btest-bg-run prog2 "btest-sqlite-driver --program=../prog2.json > out.txt"
# @TEST-EXEC: btest-bg-wait 30
# @TEST-EXEC: awk 'FNR==1 {print "==> ", FILENAME}{print}' prog*/out.txt > all.txt
# @TEST-EXEC: btest-diff all.txt

# Writes 'key1' into an SQLite file.
@TEST-START-FILE writer.json
{
    "config": {
        "file-path": "test.db",
        "options": {}
    },
    "commands": [
        ["put", ["key1", "value1"]],
        ["get", ["key1"]]
    ]
}
@TEST-END-FILE

# Fails when trying to read from a corrupt file.
@TEST-START-FILE prog1.json
{
    "config": {
        "file-path": "../writer1/test.db",
        "options": {
            "failure_mode": "Broker::SQLITE_FAILURE_MODE_FAIL"
        }
    },
    "commands": [
        ["get", ["key1"]]
    ]
}
@TEST-END-FILE

# Deletes the corrupt file and carries on.
@TEST-START-FILE prog2.json
{
    "config": {
        "file-path": "../writer2/test.db",
        "options": {
            "failure_mode": "Broker::SQLITE_FAILURE_MODE_DELETE"
        }
    },
    "commands": [
        ["get", ["key1"]]
    ]
}
@TEST-END-FILE

# Destroys a database by writing garbage into the SQLite file.
@TEST-START-FILE destroydb.py
import sys
with open(sys.argv[1], "r+b") as f:
    f.seek(2)
    f.write('AAAA'.encode('ascii'))
@TEST-END-FILE
