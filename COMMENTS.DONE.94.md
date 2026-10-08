# COMMENTS.DONE.94 — Milestone 16 Phase 4, Critic 94 round 1

All findings resolved on `milestone-16-p4fix` (plain commits, merged back with a merge commit).

- **Issue 1 (Medium, `a5269fa2`): `REBOOTSTRAP_REQUIRED` branch can panic `NetworkClient::poll`.**
  - Fixed in `2e6b2968`: `handle_completed_receives` skips a receive whose node has no in-flight requests
    and is disconnected. Only this loop's rebootstrap branch can produce that, and `process_disconnection`
    already failed the node's requests.
  - Recorded as a deviation: Java throws from `completeNext`, and its I/O threads catch it.
  - Critic 94's probe is the regression test, in both orders
    (`test_rebootstrap_required_skips_a_later_receive_from_a_node_it_disconnected`,
    `test_rebootstrap_required_after_a_receive_from_another_node`). With the skip disabled it panics at
    `in_flight_requests.rs:307`.
- **Issue 2 (Low, `0212ac53`): `cluster_check_test` stops at the rebootstrap.** Fixed in `55e8e585`.
  `assert_recovers` checks that the metadata refreshes with the real cluster id and the real broker becomes
  ready. It runs on 4.4 for both the wrong-cluster-id and the wrong-node-id tests. 3/3 on 4.4.0-rc4 and
  on 4.2.0.
- **Issue 3 (Low doc, `a5269fa2`): misplaced doc line.** Fixed in `2e6b2968`.
- **N2 (KAFKA-19117).** Fixed in `d16e956e`: the throttle log is at warn with Java's text.
- **Q1, Q2.** Recorded in the Phase 4 notes as open questions for the human; not implemented.
- **N1 (`parse_bool`).** Left to config audit #28, as the Critic asked.
