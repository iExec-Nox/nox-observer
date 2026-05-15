-- Idempotent insert of a parent-child edge, used by subgraph_syncer.
-- The parent row in `handles` must exist beforehand (foreign key enforced);
-- callers should upsert the parent handle first, then call this for each edge.

INSERT INTO handle_parents (child_handle_id, parent_handle_id)
VALUES ($1, $2)
ON CONFLICT (child_handle_id, parent_handle_id) DO NOTHING;
