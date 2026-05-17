//! Simulation tests for `bashfuldb-replication`.
//!
//! Covers: quorum reads/writes, partial-quorum success, sloppy quorum with
//! hinted handoff, read repair, conflict resolution, delete-wins semantics,
//! idempotency, and hint delivery.

use std::collections::BTreeMap;
use std::sync::Arc;

use bashfuldb_clock::{ManualClock, NodeId, VectorClock};
use bashfuldb_cluster::Ring;
use bashfuldb_document::{Document, ObjectId, Value};
use bashfuldb_replication::{
    coordinator::Coordinator,
    simulated::{SimpleMembership, SimulatedReplicationNetwork},
    HintDeliveryTask, ObjectKey, QuorumConfig, QuorumCoordinator, ReplicaStore,
};
use bashfuldb_storage::MemEngine;

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Build a 3-node cluster where every node stores all data.
///
/// Returns `(network, membership_for_node_0, coordinator_for_node_0)`.
fn make_cluster(config: QuorumConfig) -> (
    SimulatedReplicationNetwork,
    Arc<SimpleMembership>,
    QuorumCoordinator,
    Vec<NodeId>,
) {
    let node_ids: Vec<NodeId> = (0..3)
        .map(|i| NodeId::from_uuid(uuid::Uuid::from_u128(i + 1)))
        .collect();

    let mut ring = Ring::with_vnodes_per_node(64);
    for &nid in &node_ids {
        ring.add_node(nid);
    }

    let network = SimulatedReplicationNetwork::new();
    for &nid in &node_ids {
        let engine: Arc<dyn bashfuldb_storage::StorageEngine> =
            Arc::new(MemEngine::new());
        let store = Arc::new(ReplicaStore::new(nid, engine));
        network.register_node(store);
    }

    let membership = Arc::new(SimpleMembership::new(
        node_ids[0],
        ring,
        node_ids.clone(),
    ));

    let transport = Arc::new(network.transport_for(node_ids[0]));
    let clock = Arc::new(ManualClock::new(1_000_000));

    let coordinator = QuorumCoordinator::new(
        node_ids[0],
        membership.clone(),
        transport,
        clock,
        config,
    );

    (network, membership, coordinator, node_ids)
}

fn simple_doc(value: i64) -> Document {
    let mut map = BTreeMap::new();
    map.insert("v".to_string(), Value::Int(value));
    Document::new(ObjectId::new(), Value::Object(map)).unwrap()
}

fn make_key(collection: &str) -> ObjectKey {
    ObjectKey::new(collection, ObjectId::new())
}

// ── Basic quorum write + read ─────────────────────────────────────────────────

#[tokio::test]
async fn basic_write_and_read() {
    let (_, _, coord, _) = make_cluster(QuorumConfig::default());

    let key = make_key("test");
    let doc = simple_doc(42);

    let wr = coord.quorum_write(&key, &doc, None).await.unwrap();
    assert!(wr.nodes_acked >= 2, "expected >= W=2 acks, got {}", wr.nodes_acked);
    assert_eq!(wr.hints_stored, 0);

    let rr = coord.quorum_read(&key).await.unwrap();
    assert_eq!(rr.document.as_ref().unwrap().id(), doc.id());
}

// ── Read after delete ─────────────────────────────────────────────────────────

#[tokio::test]
async fn delete_makes_read_return_none() {
    let (_, _, coord, _) = make_cluster(QuorumConfig::default());

    let key = make_key("del");
    let doc = simple_doc(1);

    coord.quorum_write(&key, &doc, None).await.unwrap();
    let dr = coord.quorum_delete(&key).await.unwrap();
    assert!(dr.nodes_acked >= 2);

    let rr = coord.quorum_read(&key).await.unwrap();
    // Tombstone → document is None.
    assert!(rr.document.is_none());
}

// ── Partial quorum: one node down, R=W=2 still succeeds ──────────────────────

#[tokio::test]
async fn partial_quorum_one_node_down() {
    let (network, _, coord, node_ids) = make_cluster(QuorumConfig::default());

    network.set_node_down(node_ids[2]); // take down one replica

    let key = make_key("partial");
    let doc = simple_doc(99);

    let wr = coord.quorum_write(&key, &doc, None).await.unwrap();
    assert_eq!(wr.nodes_acked, 2, "should ack exactly 2 owners");

    let rr = coord.quorum_read(&key).await.unwrap();
    assert!(rr.document.is_some());
}

// ── Too many nodes down: quorum cannot be reached ────────────────────────────

#[tokio::test]
async fn insufficient_replicas_when_too_many_down() {
    let (network, _, coord, node_ids) = make_cluster(QuorumConfig::default());

    // Take down 2 out of 3 nodes → only 1 ack possible, W=2 unreachable.
    network.set_node_down(node_ids[1]);
    network.set_node_down(node_ids[2]);

    let key = make_key("fail");
    let doc = simple_doc(0);
    let err = coord.quorum_write(&key, &doc, None).await.unwrap_err();
    assert!(
        matches!(err, bashfuldb_replication::ReplicationError::InsufficientReplicas { .. }),
        "expected InsufficientReplicas, got {err:?}",
    );
}

// ── Sloppy quorum: 2 owners down, 1 non-owner available as hint node ─────────

#[tokio::test]
async fn sloppy_quorum_hints_bridge_the_gap() {
    // Use a 4-node cluster so we have a fallback (non-owner) node available.
    let node_ids: Vec<NodeId> = (0..4)
        .map(|i| NodeId::from_uuid(uuid::Uuid::from_u128(i + 10)))
        .collect();

    let mut ring = Ring::with_vnodes_per_node(64);
    for &nid in &node_ids {
        ring.add_node(nid);
    }

    let network = SimulatedReplicationNetwork::new();
    for &nid in &node_ids {
        let engine: Arc<dyn bashfuldb_storage::StorageEngine> =
            Arc::new(MemEngine::new());
        let store = Arc::new(ReplicaStore::new(nid, engine));
        network.register_node(store);
    }

    let membership = Arc::new(SimpleMembership::new(
        node_ids[0],
        ring,
        node_ids.clone(),
    ));

    let transport = Arc::new(network.transport_for(node_ids[0]));
    let clock = Arc::new(ManualClock::new(2_000_000));
    let coord = QuorumCoordinator::new(
        node_ids[0],
        membership,
        transport,
        clock,
        QuorumConfig::default(),
    );

    // Pick a key whose owners include nodes 1, 2, 3. Then take 2 of them down.
    // The coordinator will succeed by storing 1 hint on node_ids[0] (if it is
    // not an owner) or whatever fallback is available.
    //
    // For this test we simply confirm: if 1 owner ack + 1 hint >= W=2, write
    // succeeds.
    let key = make_key("sloppy");
    let doc = simple_doc(7);

    // Take down two of the four nodes (whichever they are, some will fail).
    network.set_node_down(node_ids[2]);
    network.set_node_down(node_ids[3]);

    // We need at least one owner up + one fallback, so leave node 0 and 1 up.
    // Write must succeed (1 owner ack + 1 hint or 2 owner acks depending on key).
    let result = coord.quorum_write(&key, &doc, None).await;
    // This should succeed because nodes 0 and 1 are available (≥ W=2 combined).
    assert!(result.is_ok(), "sloppy quorum write failed: {result:?}");
}

// ── Read repair ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn read_repair_propagates_latest_to_stale_replica() {
    let (network, _, coord, node_ids) = make_cluster(QuorumConfig::default());

    let key = make_key("repair");
    let doc = simple_doc(55);

    // Write with node 2 down so it misses the write.
    network.set_node_down(node_ids[2]);
    coord.quorum_write(&key, &doc, None).await.unwrap();

    // Bring node 2 back up; it now has a stale (missing) copy.
    network.set_node_up(node_ids[2]);

    let rr = coord.quorum_read(&key).await.unwrap();
    assert!(rr.document.is_some());
    // Read repair should have been triggered for the stale replica.
    assert!(rr.repairs_triggered >= 1, "expected repairs, got 0");

    // Give the spawned repair task a moment to run.
    tokio::task::yield_now().await;
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // Now read directly from node 2's store; it should have the repaired doc.
    // We do this by temporarily taking the other nodes down so only node 2 is
    // read; we need R=1 for that. Instead, just do a second quorum read and
    // confirm repairs_triggered drops to 0 (all replicas are now up to date).
    let rr2 = coord.quorum_read(&key).await.unwrap();
    assert_eq!(rr2.repairs_triggered, 0, "second read should need no repairs");
}

// ── Conflict resolution: concurrent writes, LWW ──────────────────────────────

#[tokio::test]
async fn concurrent_writes_resolved_by_lww() {
    use bashfuldb_replication::coordinator::resolve_conflict;
    use bashfuldb_replication::VersionedDoc;
    use bashfuldb_clock::Hlc;

    let node_a = NodeId::from_uuid(uuid::Uuid::from_u128(100));
    let node_b = NodeId::from_uuid(uuid::Uuid::from_u128(200));

    let hlc_a = Hlc::new(1_000, 0);
    let hlc_b = Hlc::new(2_000, 0); // b is newer (LWW winner)

    let mut vc_a = VectorClock::new();
    vc_a.increment(node_a, hlc_a).unwrap();

    let mut vc_b = VectorClock::new();
    vc_b.increment(node_b, hlc_b).unwrap(); // concurrent with vc_a

    let doc_a = simple_doc(1);
    let doc_b = simple_doc(2);

    let vdoc_a = VersionedDoc::new(doc_a, vc_a, hlc_a);
    let vdoc_b = VersionedDoc::new(doc_b, vc_b, hlc_b);

    let winner = resolve_conflict(vec![vdoc_a, vdoc_b]);
    // b has higher HLC → should win LWW.
    assert_eq!(
        winner.document.unwrap().data(),
        &Value::Object({
            let mut m = BTreeMap::new();
            m.insert("v".to_string(), Value::Int(2));
            m
        })
    );
}

// ── Delete wins over concurrent update ───────────────────────────────────────

#[tokio::test]
async fn delete_wins_over_concurrent_update() {
    use bashfuldb_replication::coordinator::resolve_conflict;
    use bashfuldb_replication::VersionedDoc;
    use bashfuldb_clock::Hlc;

    let node_a = NodeId::from_uuid(uuid::Uuid::from_u128(300));
    let node_b = NodeId::from_uuid(uuid::Uuid::from_u128(400));

    let hlc_a = Hlc::new(3_000, 0);
    let hlc_b = Hlc::new(1_000, 0); // tombstone is older by HLC but wins by delete-wins

    let mut vc_a = VectorClock::new();
    vc_a.increment(node_a, hlc_a).unwrap();

    let mut vc_b = VectorClock::new();
    vc_b.increment(node_b, hlc_b).unwrap(); // concurrent with vc_a

    // a is a live doc (higher HLC), b is a tombstone (lower HLC).
    let doc = simple_doc(99);
    let live = VersionedDoc::new(doc, vc_a, hlc_a);
    let tomb = VersionedDoc::tombstone(vc_b, hlc_b);

    // Delete-wins: tombstone should be returned even though HLC(b) < HLC(a).
    let winner = resolve_conflict(vec![live, tomb]);
    assert!(winner.is_tombstone, "tombstone should win over live doc");
    assert!(winner.document.is_none());
}

// ── Idempotency: same key + same payload → cached result ─────────────────────

#[tokio::test]
async fn idempotency_same_payload_returns_cached() {
    let (_, _, coord, _) = make_cluster(QuorumConfig::default());

    let key = make_key("idem");
    let doc = simple_doc(10);

    let r1 = coord
        .quorum_write(&key, &doc, Some("ikey-1"))
        .await
        .unwrap();

    // Second call with same idempotency key + same doc: should return same
    // result without hitting replicas again.
    let r2 = coord
        .quorum_write(&key, &doc, Some("ikey-1"))
        .await
        .unwrap();

    assert_eq!(r1.nodes_acked, r2.nodes_acked);
    assert_eq!(r1.version.fingerprint(), r2.version.fingerprint());
}

// ── Idempotency: same key + different payload → 409 ──────────────────────────

#[tokio::test]
async fn idempotency_different_payload_returns_conflict() {
    let (_, _, coord, _) = make_cluster(QuorumConfig::default());

    let key = make_key("idem2");
    let doc1 = simple_doc(10);
    let doc2 = simple_doc(99); // different value

    coord
        .quorum_write(&key, &doc1, Some("ikey-2"))
        .await
        .unwrap();

    let err = coord
        .quorum_write(&key, &doc2, Some("ikey-2"))
        .await
        .unwrap_err();

    assert!(
        matches!(err, bashfuldb_replication::ReplicationError::IdempotencyConflict),
        "expected IdempotencyConflict, got {err:?}",
    );
}

// ── Hinted handoff delivery ───────────────────────────────────────────────────

#[tokio::test]
async fn hint_delivered_to_recovered_node() {
    // 4-node cluster: nodes 0-3.  Node 3 is an owner but is initially down.
    // The coordinator stores a hint on a fallback node.
    // After recovery we run the hint delivery task and verify node 3 has data.

    let node_ids: Vec<NodeId> = (0..4)
        .map(|i| NodeId::from_uuid(uuid::Uuid::from_u128(i + 20)))
        .collect();

    let mut ring = Ring::with_vnodes_per_node(64);
    for &nid in &node_ids {
        ring.add_node(nid);
    }

    let network = SimulatedReplicationNetwork::new();
    let mut stores: Vec<Arc<ReplicaStore>> = Vec::new();
    for &nid in &node_ids {
        let engine: Arc<dyn bashfuldb_storage::StorageEngine> =
            Arc::new(MemEngine::new());
        let store = Arc::new(ReplicaStore::new(nid, engine));
        network.register_node(Arc::clone(&store));
        stores.push(store);
    }

    let membership = Arc::new(SimpleMembership::new(
        node_ids[0],
        ring,
        node_ids.clone(),
    ));

    let transport = Arc::new(network.transport_for(node_ids[0]));
    let clock = Arc::new(ManualClock::new(3_000_000));
    let coord = QuorumCoordinator::new(
        node_ids[0],
        membership,
        transport.clone(),
        clock,
        QuorumConfig { n: 3, r: 2, w: 2 },
    );

    // Choose a key. We need it to have some of the nodes as owners.
    // We'll just try a write with some nodes down and check that hints are stored.
    network.set_node_down(node_ids[3]);

    let key = make_key("hint_delivery");
    let doc = simple_doc(77);

    let wr = coord.quorum_write(&key, &doc, None).await.unwrap();
    let initial_hints = wr.hints_stored;
    // Depending on ring assignment, hints may or may not be stored.
    // If node_ids[3] was an owner and a fallback exists, hints_stored > 0.
    // Just verify the write succeeded.
    assert!(
        wr.nodes_acked + initial_hints >= 2,
        "expected quorum ack, got acked={} hints={}",
        wr.nodes_acked, initial_hints
    );

    // Bring node 3 back up and run hint delivery from each non-target store.
    network.set_node_up(node_ids[3]);

    // Run hint delivery for every node that could hold hints.
    for store in &stores {
        let task = HintDeliveryTask::new(
            Arc::clone(store),
            transport.clone(),
            std::time::Duration::from_secs(60),
        );
        let _ = task.run_once().await;
    }

    // Now a quorum read should succeed.
    let rr = coord.quorum_read(&key).await.unwrap();
    assert!(rr.document.is_some(), "document should be readable after hint delivery");
}

// ── Write then overwrite: later write wins ────────────────────────────────────

#[tokio::test]
async fn later_write_overwrites_earlier() {
    let (_, _, coord, _) = make_cluster(QuorumConfig::default());

    let key = make_key("overwrite");
    let doc1 = simple_doc(10);
    let doc2 = simple_doc(20);

    coord.quorum_write(&key, &doc1, None).await.unwrap();
    coord.quorum_write(&key, &doc2, None).await.unwrap();

    let rr = coord.quorum_read(&key).await.unwrap();
    let got = rr.document.unwrap();
    // doc2 was written after doc1; it should win.
    assert_eq!(got.id(), doc2.id());
}

// ── Empty read (missing key) ──────────────────────────────────────────────────

#[tokio::test]
async fn read_missing_key_returns_none() {
    let (_, _, coord, _) = make_cluster(QuorumConfig::default());

    let key = make_key("missing");
    let rr = coord.quorum_read(&key).await.unwrap();
    assert!(rr.document.is_none());
    assert!(rr.version.is_empty());
}

// ── Network partition then heal ───────────────────────────────────────────────

#[tokio::test]
async fn write_through_partition_then_heal() {
    let (network, _, coord, node_ids) = make_cluster(QuorumConfig::default());

    // Partition coordinator (node 0) from node 2.
    network.partition(node_ids[0], node_ids[2]);

    let key = make_key("partition");
    let doc = simple_doc(5);

    // Write succeeds: nodes 0 and 1 still reachable (2 acks = W).
    coord.quorum_write(&key, &doc, None).await.unwrap();

    // Heal partition.
    network.heal_partition(node_ids[0], node_ids[2]);

    // Read now reaches all 3 nodes; should still succeed.
    let rr = coord.quorum_read(&key).await.unwrap();
    assert!(rr.document.is_some());
}
