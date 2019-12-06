-- first create madlib_perm_series:
create table madlib_perm_series AS SELECT generate_series(0,99999) as buffer_id distributed by (buffer_id);
-- then create madlib_perm_dist_key
create table madlib_perm_dist_key AS SELECT MIN(buffer_id) AS dist_key FROM madlib_perm_series GROUP BY gp_segment_id DISTRIBUTED BY (dist_key);

-- now join the two:
create table madlib_perm_series_distkey as select dist_key, buffer_id from madlib_perm_dist_key d join madlib_perm_series b on d.gp_segment_id = b.gp_segment_id distributed by (buffer_id);

-- and add row numbering as "buffer_index":


-- (above two we already do, both are nearly instantaneous)

-- Then create redistributed and shuffled (and probably, normalized and one-hot-encoded at same time) table:

-- 24627 = COUNT(*) / num_segments
TODO: try % instead of /
create table redist100_with_distkey as select p.x, p.y, d.dist_key FROM places100_train As p JOIN madlib_perm_dist_key as d ON (p.id % 20) = d.gp_segment_id ORDER BY RANDOM() DISTRIBUTED BY (dist_key);
-- 2 min 13s for places100

explain create table redist100_with_distkey_rowid as select x, y, dist_key, (ROW_NUMBER() OVER(PARTITION BY dist_key))::INTEGER as slot_id FROM redist100_with_distkey distributed by (dist_key); -- 64s places100  (4s places10)
-- 67s for places100

-- TODO: rename to start_rows instead of rows_per_segment
explain create table rows_per_segment100 as select dist_key, sum(rows_per_seg) over(order by seg_id) as start_row_id from (select gp_segment_id as seg_id, count(*) as rows_per_seg, min(dist_key) as dist_key from redist100_with_distkey_rowid group by gp_segment_id order by gp_segment_id) a distributed by (dist_key); -- 0.1s places100

-- create table foo as select p.gp_segment_id as seg_id, start_row_id + row_id - 24713 as row_id, p.dist_key as dist_key from redist100_with_distkey_rowid p join rows_per_segment100 r on p.dist_key=r.dist_key order by row_id distributed by (dist_key);
-- Time:  .3s  (places100)

-- 24627 =>  SELECT start_row_id FROM rows_
create table p100_numbered as select p.x, p.gp_segment_id as seg_id, start_row_id + slot_id - 24627 as row_id, p.dist_key as dist_key from redist100_with_distkey_rowid p join rows_per_segment100 r on p.dist_key=r.dist_key order by row_id distributed by (dist_key);
-- Time: 40s (places100)   (ran a similar query in 79s, then slightly modified in 53s, then exited out of sql, went back in and ran this third variant which took only 39.99s--no idea why it got faster each time)

--  To check numbering looks right:
-- select gp_segment_id, dist_key, min(row_id), max(row_id), min(slot_id), max(slot_id) from p100_numbered group by gp_segment_id, dist_key;

-- Now add buffer_id's:
--create table p100_numbered_buffer_id as select p.*, b.buffer_id from p100_numbered p join madlib_perm_series b on p.row_id::INTEGER / 382 = b.buffer_id ORDER BY buffer_id DISTRIBUTED by (dist_key, buffer_id);
-- Time:  157.2s ~ 2.5 min  (without ORDER BY buffer_id)
-- Time:  144.9s ~ 2.4 min  (with ORDER BY buffer_id)

SET gp_autostats_mode=none;   -- turn off stats generation on table creation, to be more similar to the GroupAgg that's working in v116

--create table p100_numbered_buffer_id as select p.*, b.buffer_id from p100_numbered p join madlib_perm_series b on p.row_id::INTEGER / 382 = b.buffer_id ORDER BY buffer_id DISTRIBUTED by (buffer_id);
-- Time:  173256.797 ms ~ 2.8 min

--create table p100_numbered_buffer_id as select p.*, b.buffer_id from p100_numbered p join madlib_perm_series b on p.row_id::INTEGER / 382 = b.buffer_index ORDER BY buffer_index DISTRIBUTED by (buffer_index);
-- Time: 109.8s ~ 1 min 50s

CREATE TABLE p100_numbered_buffer_id as select p.*, b.buffer_id, b.buffer_index from p100_numbered p join madlib_perm_series_distkey_buffer_index b on p.row_id::INTEGER / 382 = b.buffer_index ORDER BY buffer_index DISTRIBUTED by (dist_key);  -- Note:  evenly distributed, but buffer_id's don't match up with dist_keys since the dist_keys are based on row id's
--Time: 97s

CREATE TABLE p100_numbered_buffer_id as select p.*, b.buffer_id, b.buffer_index from p100_numbered p join madlib_perm_series_distkey_buffer_index b on p.row_id::INTEGER % 1300 = b.buffer_index ORDER BY buffer_index DISTRIBUTED by (dist_key);  -- Note:  evenly distributed, but buffer_id's don't match up with dist_keys since the dist_keys are based on row id's
Time: 45.8s, wow!

-- create table p100_numbered_buffer_id_sorted as select * from p100_numbered_buffer_id order by buffer_id distributed by (buffer_id);
-- Time: 81.6s

SET optimizer=off;
SET enable_hashagg=off;

-- Finally, GROUP BY buffer_id:
-- create table p100_batched as select madlib.agg_array_concat(ARRAY[x]) as x, dist_key, buffer_id from p100_numbered_buffer_id group by dist_key, buffer_id distributed by (buffer_id);
-- Time: 101.4 min

create table p100_batched as select madlib.agg_array_concat(ARRAY[x]) as x, buffer_id from p100_numbered_buffer_id group by buffer_id distributed by (buffer_id);
Time: ?

-- TODO: Add a query to attach dist_key to 2nd-to-last table by joining on buffer_id = b.buffer_id

create table p100_batched as select madlib.agg_array_concat(ARRAY[x]) as x, min(dist_key) as __dist_key__, buffer_id from p100_numbered_buffer_id_with_stats group by buffer_id distributed by (__dist_key__);
