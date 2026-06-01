INSERT INTO handle_parents (child_handle_id, parent_handle_id)
VALUES ($1, $2)
ON CONFLICT (child_handle_id, parent_handle_id) DO NOTHING;
