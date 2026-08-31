use crate::{
    listeners::order_book::{L2SnapshotParams, L2Snapshots},
    order_book::Coin,
    types::{L2Book, subscription::ServerResponse},
};

// Single construction path for l2Book frames so the WS stream, WS
// subscribe-time snapshots, and MQTT publishes stay byte-identical —
// the frontend parses all three with the same code.
pub(crate) fn l2_book_response(
    l2_snapshots: &L2Snapshots,
    coin: &str,
    params: &L2SnapshotParams,
    n_levels: usize,
    time: u64,
) -> Option<ServerResponse> {
    let snapshot = l2_snapshots.as_ref().get(&Coin::new(coin))?.get(params)?;
    let snapshot = snapshot.truncate(n_levels).export_inner_snapshot();
    Some(ServerResponse::L2Book(L2Book::from_l2_snapshot(coin.to_string(), snapshot, time)))
}
