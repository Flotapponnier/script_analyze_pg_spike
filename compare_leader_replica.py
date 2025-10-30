#!/usr/bin/env python3
"""
Compare what changes during spike on leader (pg00) vs replica (pg01).
"""

import json
from collections import Counter, defaultdict
from statistics import mean, median
import argparse


def load_snapshots(filepath: str):
    """Load pg_activity snapshots from JSON file."""
    with open(filepath, 'r') as f:
        return json.load(f)


def extract_metrics(snapshots):
    """Extract aggregated metrics from snapshots."""
    if not snapshots:
        return None

    load_1m = [s['load_avg_1m'] for s in snapshots]
    active_counts = [s['active_query_count'] for s in snapshots]

    wait_events = []
    wait_event_types = []
    query_durations = []
    applications = []
    long_queries = []
    lock_waits = []

    for snapshot in snapshots:
        for query in snapshot.get('queries', []):
            wait_event = query.get('wait_event', 'unknown')
            wait_type = query.get('wait_event_type', 'unknown')
            duration = query.get('query_duration_sec', 0)
            app = query.get('application_name', 'unknown')

            wait_events.append(wait_event)
            wait_event_types.append(wait_type)
            query_durations.append(duration)
            applications.append(app)

            if duration > 10:
                long_queries.append({'duration': duration, 'app': app, 'wait': wait_event, 'wait_type': wait_type})

            if wait_type == 'Lock':
                lock_waits.append({'duration': duration, 'app': app, 'wait': wait_event})

    return {
        'snapshot_count': len(snapshots),
        'load_avg': mean(load_1m),
        'active_queries_avg': mean(active_counts),
        'total_queries': len(query_durations),
        'wait_event_types': Counter(wait_event_types),
        'wait_events': Counter(wait_events),
        'applications': Counter(applications),
        'long_queries': long_queries,
        'lock_waits': lock_waits,
        'avg_duration': mean(query_durations) if query_durations else 0,
    }


def compare_changes(server_name, normal, spike):
    """Compare normal vs spike for a single server."""
    report = []

    report.append(f"# {server_name} Analysis: Normal → Spike Changes\n")
    report.append("=" * 80)
    report.append("")

    # Load changes
    load_change = spike['load_avg'] - normal['load_avg']
    load_pct = (load_change / normal['load_avg']) * 100
    report.append(f"## System Load")
    report.append(f"- Normal: {normal['load_avg']:.2f}")
    report.append(f"- Spike:  {spike['load_avg']:.2f}")
    report.append(f"- **Change: {load_change:+.2f} ({load_pct:+.1f}%)**")
    report.append("")

    # Query count changes
    query_change = spike['active_queries_avg'] - normal['active_queries_avg']
    query_pct = (query_change / normal['active_queries_avg']) * 100
    report.append(f"## Active Query Count")
    report.append(f"- Normal: {normal['active_queries_avg']:.1f}")
    report.append(f"- Spike:  {spike['active_queries_avg']:.1f}")
    report.append(f"- **Change: {query_change:+.1f} ({query_pct:+.1f}%)**")
    report.append("")

    # Long queries
    long_normal_pct = (len(normal['long_queries']) / normal['total_queries']) * 100 if normal['total_queries'] > 0 else 0
    long_spike_pct = (len(spike['long_queries']) / spike['total_queries']) * 100 if spike['total_queries'] > 0 else 0
    report.append(f"## Long Queries (>10s)")
    report.append(f"- Normal: {len(normal['long_queries'])} ({long_normal_pct:.1f}%)")
    report.append(f"- Spike:  {len(spike['long_queries'])} ({long_spike_pct:.1f}%)")
    if len(spike['long_queries']) > 0:
        report.append(f"\n**Top 5 longest in spike:**")
        sorted_long = sorted(spike['long_queries'], key=lambda x: x['duration'], reverse=True)
        for i, q in enumerate(sorted_long[:5], 1):
            report.append(f"{i}. {q['duration']:.1f}s - {q['app'][:60]} - Wait: {q['wait_type']}/{q['wait']}")
    report.append("")

    # Lock waits
    lock_normal_pct = (len(normal['lock_waits']) / normal['total_queries']) * 100 if normal['total_queries'] > 0 else 0
    lock_spike_pct = (len(spike['lock_waits']) / spike['total_queries']) * 100 if spike['total_queries'] > 0 else 0
    report.append(f"## Lock Waits")
    report.append(f"- Normal: {len(normal['lock_waits'])} ({lock_normal_pct:.1f}%)")
    report.append(f"- Spike:  {len(spike['lock_waits'])} ({lock_spike_pct:.1f}%)")

    if len(spike['lock_waits']) > 0:
        # Group by lock type
        lock_types = Counter([l['wait'] for l in spike['lock_waits']])
        report.append(f"\n**Lock types during spike:**")
        for lock_type, count in lock_types.most_common(5):
            pct = (count / len(spike['lock_waits'])) * 100
            report.append(f"- {lock_type}: {count} ({pct:.1f}%)")
    report.append("")

    # Wait event types
    report.append(f"## Wait Event Types (Top 5)")
    report.append("### Normal:")
    for event_type, count in normal['wait_event_types'].most_common(5):
        pct = (count / normal['total_queries']) * 100
        report.append(f"- {event_type}: {count} ({pct:.1f}%)")

    report.append("\n### Spike:")
    for event_type, count in spike['wait_event_types'].most_common(5):
        pct = (count / spike['total_queries']) * 100
        report.append(f"- {event_type}: {count} ({pct:.1f}%)")

    # Calculate changes
    report.append("\n### **Changes:**")
    normal_wait_pct = {event: (count / normal['total_queries'] * 100) for event, count in normal['wait_event_types'].items()}
    spike_wait_pct = {event: (count / spike['total_queries'] * 100) for event, count in spike['wait_event_types'].items()}
    all_events = set(normal_wait_pct.keys()) | set(spike_wait_pct.keys())
    wait_diffs = []
    for event in all_events:
        normal_p = normal_wait_pct.get(event, 0)
        spike_p = spike_wait_pct.get(event, 0)
        diff = spike_p - normal_p
        if abs(diff) > 1:
            wait_diffs.append((event, normal_p, spike_p, diff))
    wait_diffs.sort(key=lambda x: abs(x[3]), reverse=True)
    for event, normal_p, spike_p, diff in wait_diffs[:5]:
        report.append(f"- **{event}**: {normal_p:.1f}% → {spike_p:.1f}% ({diff:+.1f}pp)")
    report.append("")

    # Top applications
    report.append(f"## Top Applications During Spike")
    for app, count in spike['applications'].most_common(10):
        pct = (count / spike['total_queries']) * 100
        normal_count = normal['applications'].get(app, 0)
        normal_pct = (normal_count / normal['total_queries']) * 100 if normal['total_queries'] > 0 else 0
        change = pct - normal_pct
        report.append(f"- {app or '(empty)'}: {count} ({pct:.1f}%, was {normal_pct:.1f}%, {change:+.1f}pp)")
    report.append("")

    # Apps that appear only during spike
    normal_apps = set(normal['applications'].keys())
    spike_apps = set(spike['applications'].keys())
    new_apps = spike_apps - normal_apps

    if new_apps:
        report.append(f"## New Applications During Spike ({len(new_apps)} apps)")
        new_app_counts = [(app, spike['applications'][app]) for app in new_apps]
        new_app_counts.sort(key=lambda x: x[1], reverse=True)
        for app, count in new_app_counts[:15]:
            pct = (count / spike['total_queries']) * 100
            report.append(f"- {app}: {count} ({pct:.1f}%)")
        report.append("")

    # Apps that disappear
    disappeared_apps = normal_apps - spike_apps
    if disappeared_apps:
        report.append(f"## Applications Missing During Spike ({len(disappeared_apps)} apps, top 10)")
        disappeared_counts = [(app, normal['applications'][app]) for app in disappeared_apps]
        disappeared_counts.sort(key=lambda x: x[1], reverse=True)
        for app, count in disappeared_counts[:10]:
            pct = (count / normal['total_queries']) * 100
            report.append(f"- {app}: {count} queries in normal ({pct:.1f}%)")
        report.append("")

    report.append("=" * 80)
    report.append("")

    return "\n".join(report)


def compare_servers(pg00_changes, pg01_changes):
    """Compare how pg00 vs pg01 react to spike."""
    report = []

    report.append("# Leader (pg00) vs Replica (pg01) - Spike Impact Comparison\n")
    report.append("=" * 80)
    report.append("")

    report.append("## Summary of Changes During Spike\n")
    report.append("| Metric | pg00 (Leader) | pg01 (Replica) | Analysis |")
    report.append("|--------|---------------|----------------|----------|")

    # Parse changes from the text (we'll pass the metrics directly)
    # This is a summary, actual implementation needs the metrics objects

    report.append("")
    report.append("## Key Differences")
    report.append("")
    report.append("### Workload Characteristics")
    report.append("- **pg00 (Leader)**: Handles all WRITE operations + some reads")
    report.append("- **pg01 (Replica)**: Handles READ-ONLY operations")
    report.append("")

    return "\n".join(report)


def main():
    parser = argparse.ArgumentParser(description='Compare leader vs replica spike changes')
    parser.add_argument('--pg00-normal', required=True, help='pg00 normal JSON')
    parser.add_argument('--pg00-spike', required=True, help='pg00 spike JSON')
    parser.add_argument('--pg01-normal', required=True, help='pg01 normal JSON')
    parser.add_argument('--pg01-spike', required=True, help='pg01 spike JSON')
    parser.add_argument('--output', default='leader_replica_comparison.md', help='Output file')

    args = parser.parse_args()

    print("Loading data...")
    pg00_normal = extract_metrics(load_snapshots(args.pg00_normal))
    pg00_spike = extract_metrics(load_snapshots(args.pg00_spike))
    pg01_normal = extract_metrics(load_snapshots(args.pg01_normal))
    pg01_spike = extract_metrics(load_snapshots(args.pg01_spike))

    print("Analyzing pg00 (Leader)...")
    pg00_report = compare_changes("pg00 (Leader)", pg00_normal, pg00_spike)

    print("Analyzing pg01 (Replica)...")
    pg01_report = compare_changes("pg01 (Replica)", pg01_normal, pg01_spike)

    print("Creating comparison...")

    # Build final report
    final_report = []
    final_report.append("# PostgreSQL Spike Analysis - Leader vs Replica\n")
    final_report.append("=" * 80)
    final_report.append("")

    # Quick comparison table with better formatting
    final_report.append("## Quick Comparison Table\n")

    pg00_load_change = pg00_spike['load_avg'] - pg00_normal['load_avg']
    pg00_load_pct = (pg00_load_change / pg00_normal['load_avg']) * 100
    pg01_load_change = pg01_spike['load_avg'] - pg01_normal['load_avg']
    pg01_load_pct = (pg01_load_change / pg01_normal['load_avg']) * 100

    pg00_q_change = pg00_spike['active_queries_avg'] - pg00_normal['active_queries_avg']
    pg01_q_change = pg01_spike['active_queries_avg'] - pg01_normal['active_queries_avg']

    pg00_long_change = len(pg00_spike['long_queries']) - len(pg00_normal['long_queries'])
    pg01_long_change = len(pg01_spike['long_queries']) - len(pg01_normal['long_queries'])

    pg00_lock_change = len(pg00_spike['lock_waits']) - len(pg00_normal['lock_waits'])
    pg01_lock_change = len(pg01_spike['lock_waits']) - len(pg01_normal['lock_waits'])

    # Format with fixed width columns for better alignment
    final_report.append("```")
    final_report.append("Metric           | pg00 Normal | pg00 Spike | pg00 Change      | pg01 Normal | pg01 Spike | pg01 Change")
    final_report.append("-----------------|-------------|------------|------------------|-------------|------------|------------------")
    final_report.append(f"Load Avg         | {pg00_normal['load_avg']:>11.2f} | {pg00_spike['load_avg']:>10.2f} | {pg00_load_change:>+6.2f} ({pg00_load_pct:>+5.1f}%) | {pg01_normal['load_avg']:>11.2f} | {pg01_spike['load_avg']:>10.2f} | {pg01_load_change:>+6.2f} ({pg01_load_pct:>+5.1f}%)")
    final_report.append(f"Active Queries   | {pg00_normal['active_queries_avg']:>11.1f} | {pg00_spike['active_queries_avg']:>10.1f} | {pg00_q_change:>+16.1f} | {pg01_normal['active_queries_avg']:>11.1f} | {pg01_spike['active_queries_avg']:>10.1f} | {pg01_q_change:>+18.1f}")
    final_report.append(f"Long Queries     | {len(pg00_normal['long_queries']):>11d} | {len(pg00_spike['long_queries']):>10d} | {pg00_long_change:>+16d} | {len(pg01_normal['long_queries']):>11d} | {len(pg01_spike['long_queries']):>10d} | {pg01_long_change:>+18d}")
    final_report.append(f"Lock Waits       | {len(pg00_normal['lock_waits']):>11d} | {len(pg00_spike['lock_waits']):>10d} | {pg00_lock_change:>+16d} | {len(pg01_normal['lock_waits']):>11d} | {len(pg01_spike['lock_waits']):>10d} | {pg01_lock_change:>+18d}")
    final_report.append("```")

    final_report.append("")
    final_report.append("## Key Findings\n")

    # Determine which is more impacted
    if abs(pg00_load_pct) > abs(pg01_load_pct) * 1.2:
        final_report.append(f"- **Leader (pg00) is MORE impacted**: Load increased by {pg00_load_pct:.1f}% vs {pg01_load_pct:.1f}% on replica")
    elif abs(pg01_load_pct) > abs(pg00_load_pct) * 1.2:
        final_report.append(f"- **Replica (pg01) is MORE impacted**: Load increased by {pg01_load_pct:.1f}% vs {pg00_load_pct:.1f}% on leader")
    else:
        final_report.append(f"- **Both similarly impacted**: Load increased by {pg00_load_pct:.1f}% (leader) and {pg01_load_pct:.1f}% (replica)")

    # Lock analysis
    if len(pg00_spike['lock_waits']) > 0 and len(pg01_spike['lock_waits']) == 0:
        final_report.append(f"- **Leader has lock contention ({len(pg00_spike['lock_waits'])} waits), replica has NONE** - This is expected (writes on leader only)")

    # Long queries
    if len(pg00_spike['long_queries']) > len(pg01_spike['long_queries']) * 5:
        final_report.append(f"- **Leader has significantly more long queries** ({len(pg00_spike['long_queries'])} vs {len(pg01_spike['long_queries'])}) - Write operations are blocking")
    elif len(pg01_spike['long_queries']) > len(pg00_spike['long_queries']):
        final_report.append(f"- **Replica has more long queries** ({len(pg01_spike['long_queries'])} vs {len(pg00_spike['long_queries'])}) - Read queries are slow")

    final_report.append("")
    final_report.append("=" * 80)
    final_report.append("\n\n")

    # Append individual reports
    final_report.append(pg00_report)
    final_report.append("\n\n")
    final_report.append(pg01_report)

    full_report = "\n".join(final_report)

    with open(args.output, 'w') as f:
        f.write(full_report)

    print(f"\n✅ Report generated: {args.output}")
    print("\nPreview:")
    print("=" * 80)
    lines = full_report.split('\n')
    for line in lines[:50]:
        print(line)
    if len(lines) > 50:
        print(f"\n... ({len(lines) - 50} more lines in full report)")


if __name__ == '__main__':
    main()
