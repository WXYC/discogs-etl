# Example Music Data for Tests

WXYC is a freeform station. When creating test fixtures or mock data, use representative artists instead of mainstream acts like Queen, Radiohead, or The Beatles. The canonical data source is `wxyc-shared/src/test-utils/wxyc-example-data.json`. See the reference table in the org-level CLAUDE.md.

When writing inline test data or new fixture rows, use these defaults matching the repo's data structures:

**`release` table** (id, status, title, country, released, notes, data_quality, master_id, format):
```
5001,Accepted,DOGA,AR,2024-05-10,,Correct,8001,LP
5002,Accepted,Aluminum Tunes,UK,1998-09-01,,Correct,8002,CD
5003,Accepted,Moon Pix,US,1998-09-22,,Correct,8003,LP
5004,Accepted,On Your Own Love Again,US,2015-01-27,,Correct,8004,LP
5005,Accepted,Edits,US,2023,,Correct,,CD
5006,Accepted,Duke Ellington & John Coltrane,US,1963,,Correct,8005,LP
```

**`release_artist` table** (release_id, artist_id, artist_name, extra, anv, position, join_field, role):
```
5001,101,Juana Molina,0,,1,,
5002,102,Stereolab,0,,1,,
5003,103,Cat Power,0,,1,,
5004,104,Jessica Pratt,0,,1,,
5005,105,Chuquimamani-Condori,0,,1,,
5006,106,Duke Ellington,0,,1, &,
5006,107,John Coltrane,0,,2,,
5006,108,Billy Strayhorn,1,,3,,Written-By
```
Column `role` carries release-level extra-credit attribution (e.g. `Written-By`, `Producer`) for `extra=1` rows; main artists (`extra=0`) have `role` NULL. The loader reads it via `optional_csv_columns`, so a pre-role CSV still imports — the same `(extra, role)` semantics as `release_track_artist` below.

**`release_label` table** (release_id, label, catno):
```
5001,Sonamos,SON-001
5002,Duophonic,D-UHF-CD22
5003,Matador Records,OLE 325-1
5004,Drag City,DC575
5006,Impulse Records,A-30
```

**`release_track_artist` table** (release_id, track_sequence, artist_name, extra, role):
```
5006,1,Duke Ellington,0,
5006,1,Billy Strayhorn,1,Written-By
```
Columns `extra` and `role` were added per [#218](https://github.com/WXYC/discogs-etl/issues/218) to distinguish main-artist credits (`<artists>` in the source XML, `extra=0`, `role` NULL) from extra-artist credits (`<extraartists>`, `extra=1`, `role` holds the source `<role>` text). Both are additive and NULL-tolerant: pre-#55 converter CSVs (3 columns) continue to import, and pre-migration rows default to `extra=0` / `role=NULL`. Downstream consumers filter to main credits with `WHERE extra = 0`. Mirrors the `release_artist.(extra, role)` pair. Re-ETL of the three deployments is required to populate the new columns against existing rows.

**`release_track` table** (release_id, sequence, position, title, duration):
```
5001,1,A1,Cosoco,4:12
5002,1,1,Fuses,7:29
5003,1,1,American Flag,4:18
5004,1,A1,Back Baby Back,3:22
5005,1,1,Palqa,3:45
5006,1,A1,In A Sentimental Mood,4:19
```

**`library_artists.txt`**: `Juana Molina`, `Stereolab`, `Cat Power`, `Jessica Pratt`, `Chuquimamani-Condori`, `Duke Ellington`

**SQLite `library` rows** (artist, title, format): `("Juana Molina", "DOGA", "LP")`, `("Stereolab", "Aluminum Tunes", "CD")`, `("Cat Power", "Moon Pix", "LP")`, `("Jessica Pratt", "On Your Own Love Again", "LP")`, `("Chuquimamani-Condori", "Edits", "CD")`, `("Duke Ellington", "Duke Ellington & John Coltrane", "LP")`
