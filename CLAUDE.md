# mochizuki — Generic SQLite + FTS5 Database

> **★★★ CSE / Knowable Construction.** This repo operates under **Constructive Substrate Engineering** — canonical specification at [`pleme-io/theory/CONSTRUCTIVE-SUBSTRATE-ENGINEERING.md`](https://github.com/pleme-io/theory/blob/main/CONSTRUCTIVE-SUBSTRATE-ENGINEERING.md). The Compounding Directive (operational rules: solve once, load-bearing fixes only, idiom-first, models stay current, direction beats velocity) is in the org-level pleme-io/CLAUDE.md ★★★ section. Read both before non-trivial changes.


Reusable SQLite operations with FTS5 full-text search, pruning, and
JSON row output. Eliminates copy-pasted database boilerplate across
pleme-io tools.

## API

```rust
let db = Database::open(Path::new("data.db"))?;
db.execute_batch("CREATE TABLE logs (...)")?;
db.execute("INSERT INTO logs (msg) VALUES (?1)", &[&"hello"])?;
db.search_fts("logs_fts", "logs", "hello", &["t.msg"], 10)?;
db.count("logs")?;
db.prune("logs", "created_at", 30)?;
```

## Consumers

- `andro-log` — log storage with FTS5 search
- `andro-farm` — device inventory with upsert
- `andro-build` — APK size tracking history
