# screenshots/

This directory is **intentionally empty of images right now.** No screenshot has
been captured for this project yet, so nothing here is a placeholder, a mockup,
or a stand-in — a broken image in a portfolio README is worse than no image.

Below is exactly what to capture, the command that produces it, and what a
reader is meant to take from it. Each entry names the file the main README's
**Results & Evidence** section expects.

Capture on a machine with the stack up (`make up`), from a clean run
(`make run-clean`), so the numbers match what the README claims.

---

## The five that matter

### 1. `pipeline-success.png`

```bash
make run-clean
```

Capture the tail of the run: the staging counts, the core/fact loads, and
`"status": "SUCCESS"`. What it demonstrates: the whole pipeline — ingest,
stage, dimension merges, facts, DQ gate, mart — completing end to end from an
empty database, with per-stage row counts and quarantine counts visible.

Frame it so the reader can see **stage names next to row counts**. That is the
part that reads as a real pipeline rather than a script.

### 2. `analytics-results.png`

```bash
psql -d warehouse -f sql/analytics/03_power_upgrade_before_after.sql
```

Use **query 03**, not query 01. It is the one that shows the SCD Type 2 payoff:
charge points upgraded 30 kW → 60 kW, with average kWh per session and average
power before and after the upgrade date. What it demonstrates: point-in-time
dimension joins working — a number that would collapse to zero if the
dimension were Type 1.

If you want a second analytics shot, `09_roaming_vs_own_network_mix.sql` is the
next most interesting, because it shows the roaming-margin trap being avoided.

### 3. `data-quality.png`

```bash
make dq-report
```

Or the gate summary directly:

```sql
SELECT severity, status, count(*) FROM dq.check_result
GROUP BY severity, status ORDER BY severity, status;
```

What it demonstrates: every error-severity rule passing, the honest warnings
still visible rather than suppressed, and the quarantine counts per rule. The
point is not "all green" — it is that failures are *visible and attributed*.

### 4. `idempotency.png`

```bash
make run        # a second time, over the same window
```

Capture the checksum comparison showing zero differences, and ideally the
ingest line showing `files_loaded=0  skipped_duplicate=14`. What it
demonstrates: re-running the pipeline changes no data, and the file-hash
registry skips already-ingested files rather than reloading them.

This is the single most valuable screenshot for an interviewer, because
"my pipeline is idempotent" is a claim and this is evidence.

### 5. `airflow-dags.png`

<http://localhost:8080> → the DAGs list, with all four DAGs visible and their
schedules. What it demonstrates: four real DAGs, one of them **Dataset**-
scheduled rather than cron — the cross-DAG dependency the scheduler
understands.

A good second shot is the `volthive_build_warehouse` **graph view**, which
shows the TaskGroups and the DQ gate sitting between the facts and the mart.
Name that one `airflow-build-graph.png`.

---

## Before you commit any image

- **No credentials.** Check the terminal prompt, window title, any visible
  `.env`, connection strings, and `PGPASSWORD` in scrollback.
- **No personal paths.** `C:\Users\<yourname>\...` in a title bar is worth
  cropping out. Run from a short path or crop the chrome.
- **No Claude / chat windows**, no usage or session-limit banners, no
  debugging scratch.
- **One shot per thing.** Two screenshots of the same output is noise.
- **Readable at GitHub's width.** README images render around 800 px wide;
  a 4K full-desktop capture becomes unreadable. Crop to the terminal or panel,
  not the whole screen.
- **PNG, not JPEG.** Text screenshots compress badly as JPEG.

## After you add them

The main README already has a **Results & Evidence** section with the measured
numbers. Add the image lines under the matching bullets — the section carries a
short HTML comment showing exactly where each goes.
