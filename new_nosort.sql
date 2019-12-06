CREATE TABLE madlib_perm_series AS SELECT generate_series(0,99999) as buffer_id distributed by (buffer_id);
CREATE TABLE madlib_perm_dist_key AS SELECT MIN(buffer_id) AS dist_key FROM madlib_perm_series GROUP BY gp_segment_id DISTRIBUTED BY (dist_key);
CREATE TABLE redist100_with_distkey as select p.x, p.y, d.dist_key FROM places100_train As p JOIN madlib_perm_dist_key as d ON (p.id % 20) = d.gp_segment_id ORDER BY RANDOM() DISTRIBUTED BY (dist_key);
CREATE TABLE redist100_with_distkey_slotid as select x, y, dist_key, (ROW_NUMBER() OVER(PARTITION BY dist_key))::INTEGER as slot_id FROM redist100_with_distkey distributed by (dist_key);

SET gp_autostats_mode=none;
SET optimizer=off;
SET enable_hashagg=off;

CREATE TABLE p100_batched AS SELECT dist_key, gp_segment_id * 20 + slot_id % 65 AS buffer_id normalized n GROUP BY buffer_id DISTRIBUTED BY (dist_key);

