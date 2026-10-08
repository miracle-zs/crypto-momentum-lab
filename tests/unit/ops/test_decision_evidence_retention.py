import sqlite3

from deploy.ops.archive_table import retention_row_predicate


def test_old_checkpoint_protects_only_its_own_row_and_pending_exits():
    db = sqlite3.connect(":memory:")
    db.executescript("""
        CREATE TABLE decision_traces (decision_id TEXT PRIMARY KEY);
        CREATE TABLE durable_policy_states (last_decision_id TEXT);
        CREATE TABLE durable_decision_exits (decision_id TEXT, status TEXT);
        INSERT INTO decision_traces VALUES
          ('old-head'),('ordinary'),('pending'),('sent');
        INSERT INTO durable_policy_states VALUES ('old-head');
        INSERT INTO durable_decision_exits VALUES
          ('pending','PENDING'),('sent','DISPATCHED');
    """)
    query = (
        "SELECT decision_id FROM decision_traces WHERE true"
        + retention_row_predicate("decision_traces")
    )
    assert {row[0] for row in db.execute(query)} == {"ordinary", "sent"}
    db.execute(
        "UPDATE durable_decision_exits SET status='DISPATCHED' "
        "WHERE decision_id='pending'"
    )
    assert {row[0] for row in db.execute(query)} == {"ordinary", "sent", "pending"}
    db.close()
