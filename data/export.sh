sqlite3 hn.sqlite <<'SQL'
.headers on
.mode csv
.once stories.csv
SELECT * FROM stories;
SQL

sqlite3 hn.sqlite <<'SQL'
.headers on
.mode csv
.once snapshots.csv
SELECT * FROM snapshots;
SQL

sqlite3 hn.sqlite <<'SQL'
.headers on
.mode csv
.once snapshot_targets.csv
SELECT * FROM snapshot_targets;
SQL
