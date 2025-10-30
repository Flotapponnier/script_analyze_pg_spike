# PostgreSQL Activity Snapshot Extractor & Spike Analyzer

Extract and analyze pg_activity snapshots from PostgreSQL servers (leader + replica) via SSH to diagnose performance spikes.

## Quick Start

```bash
# 1. Configure SSH credentials (first time only)
cp .env.example .env
# Edit .env with your PostgreSQL SSH credentials (pg00/pg01)

# 2. Run the interactive analysis
./analyze_spike_interactive.sh
```

When prompted, enter the dates in format **YYYY-MM-DDTHH:MM**:

### Example Test Commands

```
Spike period (when problem occurred):
  Enter spike START time: 2025-10-26T17:13
  Enter spike END time: 2025-10-26T17:18

Normal period (baseline reference):
  Enter normal START time: 2025-10-27T03:00
  Enter normal END time: 2025-10-27T03:05
```

The script will:
1. **Automatically extract data from BOTH pg00 (leader) and pg01 (replica)**
2. **Automatically run analysis and generate report**

Output:
- 4 JSON files (spike + normal for pg00 and pg01)
- 1 markdown report: `spike_analysis_YYYYMMDD_HHMMSS.md`

## Manual Extraction

If you need to extract data manually from a specific server:

```bash
./extract_pg_activity_interval.sh <start_time> <end_time> <pg00|pg01> [output_file]

# Examples:
./extract_pg_activity_interval.sh "2025-10-27T16:00" "2025-10-27T16:05" pg00 spike.json
./extract_pg_activity_interval.sh "2025-10-27T03:00" "2025-10-27T03:05" pg01 normal.json
```

## What the Analysis Reports

The generated markdown report includes:

### Leader (pg00) vs Replica (pg01) Comparison
- System load changes during spike
- Active query count changes
- Long-running queries (>10s)
- Lock contention (writes on leader only)
- Wait events breakdown
- Top applications causing load
- New applications appearing during spike

### Key Metrics Analyzed
- **Load Average**: System CPU load (1min avg)
- **Active Queries**: Number of concurrent running queries
- **Long Queries**: Queries taking >10 seconds
- **Lock Waits**: Transaction locks (transactionid, tuple, relation)
- **Wait Events**: What queries are waiting for (CPU, IO, Lock, etc.)
- **Applications**: Which services are causing the most load

## Understanding the Output

### Normal Behavior
- **pg00 (Leader)**: Handles WRITES + reads, has lock waits
- **pg01 (Replica)**: Handles READS only, no locks

### During a Spike
- Watch for **lock contention** on pg00 (leader)
- Watch for **increased read load** on pg01 (replica)
- Compare **new applications** that appear only during spike
- Look for **long-running queries** and their wait events

## Files Generated

```
spike_pg00_YYYY-MM-DDTHH-MM_YYYY-MM-DDTHH-MM.json  # Leader spike data
normal_pg00_YYYY-MM-DDTHH-MM_YYYY-MM-DDTHH-MM.json # Leader baseline
spike_pg01_YYYY-MM-DDTHH-MM_YYYY-MM-DDTHH-MM.json  # Replica spike data
normal_pg01_YYYY-MM-DDTHH-MM_YYYY-MM-DDTHH-MM.json # Replica baseline
spike_analysis_YYYYMMDD_HHMMSS.md                  # Final report
```

## Architecture

```
┌─────────────────────────────────────────┐
│  analyze_spike_interactive.sh           │
│  (User inputs spike & normal periods)   │
└──────────────┬──────────────────────────┘
               │
               ├─► extract_pg_activity_interval.sh (pg00 spike)
               ├─► extract_pg_activity_interval.sh (pg00 normal)
               ├─► extract_pg_activity_interval.sh (pg01 spike)
               ├─► extract_pg_activity_interval.sh (pg01 normal)
               │
               └─► compare_leader_replica.py
                   (Analyzes all 4 files)
                   │
                   └─► spike_analysis_YYYYMMDD_HHMMSS.md
```

## Tips for Choosing Periods

### Good Baseline (Normal) Periods
- Early morning (3am - 5am) when traffic is lowest
- Same day of week as spike (similar workload pattern)
- Avoid deployment times or known maintenance windows

### Spike Period
- Exact time when performance degraded
- Keep it short (5-10 minutes) for focused analysis
- Check your monitoring (Datadog, etc.) for exact timestamps

## Troubleshooting

### "No data found for the specified time range"
- Logs might not be available for that period yet
- Check log retention: `ssh user@host "ls -lh /var/log/pg-activity-snapshots/"`
- Try a different time range

### SSH Connection Issues
- Ensure `.env` is configured correctly
- Test manual SSH: `ssh user@host "ls /var/log/pg-activity-snapshots/"`
- Check SSH keys are properly configured
