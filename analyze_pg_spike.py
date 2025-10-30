#!/usr/bin/env python3
"""
Analyze PostgreSQL spike by comparing normal and spike periods.
Generates a detailed report of differences in queries, wait events, and system load.
"""

import json
import sys
from collections import Counter, defaultdict
from typing import Dict, List, Any
from statistics import mean, median
import argparse


def load_snapshots(filepath: str) -> List[Dict]:
    """Load pg_activity snapshots from JSON file."""
    with open(filepath, 'r') as f:
        return json.load(f)


def get_query_signature(query: Dict) -> str:
    """Extract normalized query signature."""
    preview = query.get('query_preview', '')
    if not preview:
        return None
    # Normalize: take first 200 chars, remove extra whitespace
    sig = ' '.join(preview[:200].split())
    return sig


def extract_metrics(snapshots: List[Dict]) -> Dict[str, Any]:
    """Extract aggregated metrics from snapshots."""

    # System load metrics
    load_1m = [s['load_avg_1m'] for s in snapshots]
    load_5m = [s['load_avg_5m'] for s in snapshots]
    load_15m = [s['load_avg_15m'] for s in snapshots]

    # Query count metrics
    active_counts = [s['active_query_count'] for s in snapshots]

    # Wait events
    wait_events = []
    wait_event_types = []
    query_durations = []
    transaction_durations = []
    applications = []
    query_patterns = []
    query_signatures = set()
    users = []

    # Lock analysis
    lock_waits = []
    long_queries = []

    for snapshot in snapshots:
        for query in snapshot.get('queries', []):
            wait_event = query.get('wait_event', 'unknown')
            wait_type = query.get('wait_event_type', 'unknown')
            duration = query.get('query_duration_sec', 0)
            app = query.get('application_name', 'unknown')

            wait_events.append(wait_event)
            wait_event_types.append(wait_type)
            query_durations.append(duration)
            transaction_durations.append(query.get('transaction_duration_sec', 0))
            applications.append(app)
            users.append(query.get('usename', 'unknown'))

            # Extract query pattern (first 15 words)
            preview = query.get('query_preview', '')
            if preview:
                normalized = ' '.join(preview.split()[:15])
                query_patterns.append(normalized)

                # Query signature for uniqueness analysis
                sig = get_query_signature(query)
                if sig:
                    query_signatures.add(sig)

            # Long queries
            if duration > 10:
                long_queries.append({
                    'duration': duration,
                    'app': app,
                    'wait': wait_event,
                    'wait_type': wait_type,
                    'preview': preview[:150]
                })

            # Lock waits
            if wait_type == 'Lock':
                lock_waits.append({
                    'duration': duration,
                    'app': app,
                    'wait': wait_event,
                    'preview': preview[:150]
                })

    return {
        'snapshot_count': len(snapshots),
        'load': {
            '1m': {'min': min(load_1m), 'max': max(load_1m), 'avg': mean(load_1m), 'median': median(load_1m)},
            '5m': {'min': min(load_5m), 'max': max(load_5m), 'avg': mean(load_5m), 'median': median(load_5m)},
            '15m': {'min': min(load_15m), 'max': max(load_15m), 'avg': mean(load_15m), 'median': median(load_15m)},
        },
        'active_queries': {
            'min': min(active_counts),
            'max': max(active_counts),
            'avg': mean(active_counts),
            'median': median(active_counts),
        },
        'wait_events': Counter(wait_events),
        'wait_event_types': Counter(wait_event_types),
        'query_durations': query_durations,
        'transaction_durations': transaction_durations,
        'applications': Counter(applications),
        'users': Counter(users),
        'query_patterns': Counter(query_patterns),
        'query_signatures': query_signatures,
        'total_queries_observed': len(query_durations),
        'long_queries': long_queries,
        'lock_waits': lock_waits,
    }


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format."""
    if seconds < 1:
        return f"{seconds*1000:.0f}ms"
    elif seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    else:
        return f"{seconds/3600:.1f}h"


def compare_metrics(normal: Dict, spike: Dict) -> str:
    """Generate comparison report."""

    report = []
    report.append("# PostgreSQL Spike Analysis Report - DETAILED\n")
    report.append("=" * 80)
    report.append("\n")

    # Summary
    report.append("## Executive Summary\n")
    report.append(f"- **Normal period**: {normal['snapshot_count']} snapshots, {normal['total_queries_observed']} queries observed")
    report.append(f"- **Spike period**: {spike['snapshot_count']} snapshots, {spike['total_queries_observed']} queries observed")
    report.append(f"- **Unique query patterns**: Normal={len(normal['query_signatures'])}, Spike={len(spike['query_signatures'])}")
    report.append(f"- **Long queries (>10s)**: Normal={len(normal['long_queries'])}, Spike={len(spike['long_queries'])}")
    report.append(f"- **Lock waits**: Normal={len(normal['lock_waits'])}, Spike={len(spike['lock_waits'])}")
    report.append("")

    # System Load Comparison
    report.append("## System Load Analysis\n")
    report.append("### Load Average (1 minute)")
    report.append(f"- Normal: min={normal['load']['1m']['min']:.2f}, max={normal['load']['1m']['max']:.2f}, "
                  f"avg={normal['load']['1m']['avg']:.2f}, median={normal['load']['1m']['median']:.2f}")
    report.append(f"- Spike:  min={spike['load']['1m']['min']:.2f}, max={spike['load']['1m']['max']:.2f}, "
                  f"avg={spike['load']['1m']['avg']:.2f}, median={spike['load']['1m']['median']:.2f}")

    load_diff = spike['load']['1m']['avg'] - normal['load']['1m']['avg']
    load_pct = (load_diff / normal['load']['1m']['avg']) * 100 if normal['load']['1m']['avg'] > 0 else 0
    report.append(f"- **Change**: {load_diff:+.2f} ({load_pct:+.1f}%)")
    report.append("")

    # Active Queries
    report.append("## Active Query Count\n")
    report.append(f"- Normal: min={normal['active_queries']['min']}, max={normal['active_queries']['max']}, "
                  f"avg={normal['active_queries']['avg']:.1f}, median={normal['active_queries']['median']:.1f}")
    report.append(f"- Spike:  min={spike['active_queries']['min']}, max={spike['active_queries']['max']}, "
                  f"avg={spike['active_queries']['avg']:.1f}, median={spike['active_queries']['median']:.1f}")

    query_diff = spike['active_queries']['avg'] - normal['active_queries']['avg']
    query_pct = (query_diff / normal['active_queries']['avg']) * 100 if normal['active_queries']['avg'] > 0 else 0
    report.append(f"- **Change**: {query_diff:+.1f} ({query_pct:+.1f}%)")
    report.append("")

    # Query Duration Analysis
    report.append("## Query Duration Analysis\n")
    if normal['query_durations'] and spike['query_durations']:
        normal_avg = mean(normal['query_durations'])
        spike_avg = mean(spike['query_durations'])
        normal_median = median(normal['query_durations'])
        spike_median = median(spike['query_durations'])
        normal_max = max(normal['query_durations'])
        spike_max = max(spike['query_durations'])

        report.append(f"- Normal: avg={format_duration(normal_avg)}, median={format_duration(normal_median)}, max={format_duration(normal_max)}")
        report.append(f"- Spike:  avg={format_duration(spike_avg)}, median={format_duration(spike_median)}, max={format_duration(spike_max)}")

        duration_diff = spike_avg - normal_avg
        duration_pct = (duration_diff / normal_avg) * 100 if normal_avg > 0 else 0
        report.append(f"- **Change**: {format_duration(abs(duration_diff))} ({duration_pct:+.1f}%)")
    report.append("")

    # LONG QUERIES ANALYSIS
    report.append("## Long-Running Queries (>10s)\n")
    report.append(f"### Spike Period: {len(spike['long_queries'])} long queries")
    if spike['long_queries']:
        spike['long_queries'].sort(key=lambda x: x['duration'], reverse=True)
        report.append("\n**Top 10 longest queries:**")
        for i, q in enumerate(spike['long_queries'][:10], 1):
            report.append(f"\n{i}. **{format_duration(q['duration'])}** | Wait: {q['wait_type']}/{q['wait']}")
            report.append(f"   App: {q['app'][:60]}")
            report.append(f"   Query: {q['preview']}...")
    report.append("")

    # LOCK ANALYSIS
    report.append("## Lock Contention Analysis\n")
    report.append(f"- Normal period: {len(normal['lock_waits'])} lock waits")
    report.append(f"- Spike period: {len(spike['lock_waits'])} lock waits")

    if spike['lock_waits']:
        # Group by lock type
        lock_by_event = defaultdict(list)
        for lock in spike['lock_waits']:
            lock_by_event[lock['wait']].append(lock)

        report.append("\n### Lock Types During Spike:")
        for lock_type in ['transactionid', 'tuple', 'relation']:
            locks = lock_by_event[lock_type]
            if not locks:
                continue

            report.append(f"\n**{lock_type} locks: {len(locks)} occurrences**")

            # Group by app
            by_app = defaultdict(int)
            for lock in locks:
                by_app[lock['app']] += 1

            report.append("Top apps waiting:")
            for app, count in sorted(by_app.items(), key=lambda x: x[1], reverse=True)[:5]:
                pct = (count / len(locks)) * 100
                report.append(f"  - {app[:60]}: {count} ({pct:.1f}%)")
    report.append("")

    # UNIQUE QUERIES ANALYSIS
    report.append("## Unique Query Analysis\n")
    both = normal['query_signatures'] & spike['query_signatures']
    only_in_spike = spike['query_signatures'] - normal['query_signatures']
    only_in_normal = normal['query_signatures'] - spike['query_signatures']

    report.append(f"- Queries in BOTH periods: {len(both)}")
    report.append(f"- Queries ONLY in normal: {len(only_in_normal)}")
    report.append(f"- Queries ONLY in spike: {len(only_in_spike)}")

    if only_in_spike:
        report.append(f"\n### New queries appearing during spike (first 5):")
        for i, query in enumerate(list(only_in_spike)[:5], 1):
            report.append(f"{i}. {query[:120]}...")
    report.append("")

    # APPLICATION ANALYSIS
    report.append("## Application Analysis\n")

    # Apps unique to spike
    normal_apps = set(normal['applications'].keys())
    spike_apps = set(spike['applications'].keys())
    only_spike_apps = spike_apps - normal_apps
    only_normal_apps = normal_apps - spike_apps

    if only_spike_apps:
        report.append(f"### Applications appearing ONLY during spike ({len(only_spike_apps)} apps):")
        spike_app_counts = [(app, spike['applications'][app]) for app in only_spike_apps]
        spike_app_counts.sort(key=lambda x: x[1], reverse=True)
        for app, count in spike_app_counts[:15]:
            report.append(f"- **{app}**: {count} queries")

    if only_normal_apps:
        report.append(f"\n### Applications disappearing during spike ({len(only_normal_apps)} apps, top 10):")
        normal_app_counts = [(app, normal['applications'][app]) for app in only_normal_apps]
        normal_app_counts.sort(key=lambda x: x[1], reverse=True)
        for app, count in normal_app_counts[:10]:
            report.append(f"- **{app}**: {count} queries in normal period")

    report.append("\n### Top Applications Overall")
    report.append("**Normal Period:**")
    for app, count in normal['applications'].most_common(10):
        pct = (count / normal['total_queries_observed'] * 100) if normal['total_queries_observed'] > 0 else 0
        report.append(f"- {app or '(empty)'}: {count} ({pct:.1f}%)")

    report.append("\n**Spike Period:**")
    for app, count in spike['applications'].most_common(10):
        pct = (count / spike['total_queries_observed'] * 100) if spike['total_queries_observed'] > 0 else 0
        report.append(f"- {app or '(empty)'}: {count} ({pct:.1f}%)")
    report.append("")

    # Wait Events Analysis
    report.append("## Wait Events Analysis\n")
    report.append("### Top Wait Event Types\n")
    report.append("**Normal Period:**")
    for event_type, count in normal['wait_event_types'].most_common(10):
        pct = (count / normal['total_queries_observed'] * 100) if normal['total_queries_observed'] > 0 else 0
        report.append(f"- {event_type}: {count} ({pct:.1f}%)")

    report.append("\n**Spike Period:**")
    for event_type, count in spike['wait_event_types'].most_common(10):
        pct = (count / spike['total_queries_observed'] * 100) if spike['total_queries_observed'] > 0 else 0
        report.append(f"- {event_type}: {count} ({pct:.1f}%)")

    report.append("\n### Top Specific Wait Events\n")
    report.append("**Normal Period:**")
    for event, count in normal['wait_events'].most_common(10):
        pct = (count / normal['total_queries_observed'] * 100) if normal['total_queries_observed'] > 0 else 0
        report.append(f"- {event}: {count} ({pct:.1f}%)")

    report.append("\n**Spike Period:**")
    for event, count in spike['wait_events'].most_common(10):
        pct = (count / spike['total_queries_observed'] * 100) if spike['total_queries_observed'] > 0 else 0
        report.append(f"- {event}: {count} ({pct:.1f}%)")
    report.append("")

    # Key Findings
    report.append("## Key Findings & Recommendations\n")
    findings = []

    # Long queries finding
    if len(spike['long_queries']) > len(normal['long_queries']) * 1.5:
        findings.append(f"- **CRITICAL**: Long-running queries increased significantly ({len(normal['long_queries'])} → {len(spike['long_queries'])})")

    # Lock contention finding
    if len(spike['lock_waits']) > len(normal['lock_waits']) * 1.2:
        findings.append(f"- **HIGH**: Lock contention increased ({len(normal['lock_waits'])} → {len(spike['lock_waits'])})")

    # Load findings
    if abs(load_pct) > 10:
        direction = "increased" if load_pct > 0 else "decreased"
        findings.append(f"- System load {direction} by {abs(load_pct):.1f}% during spike")

    # New apps finding
    if only_spike_apps:
        findings.append(f"- {len(only_spike_apps)} new applications appeared during spike (check for scheduled jobs, manual queries)")

    # Top culprit app
    if spike['long_queries']:
        culprit_apps = Counter([q['app'] for q in spike['long_queries']])
        top_culprit = culprit_apps.most_common(1)[0]
        findings.append(f"- **Main culprit**: '{top_culprit[0]}' responsible for {top_culprit[1]} long queries")

    if findings:
        report.extend(findings)
    else:
        report.append("- No significant differences detected between normal and spike periods")

    report.append("")
    report.append("=" * 80)

    return "\n".join(report)


def main():
    parser = argparse.ArgumentParser(description='Analyze PostgreSQL spike from pg_activity snapshots')
    parser.add_argument('--normal-json', required=True, help='Path to normal period JSON file')
    parser.add_argument('--spike-json', required=True, help='Path to spike period JSON file')
    parser.add_argument('--output', default='report.md', help='Output report file (default: report.md)')

    args = parser.parse_args()

    print(f"Loading normal period: {args.normal_json}")
    normal_snapshots = load_snapshots(args.normal_json)
    print(f"  → Loaded {len(normal_snapshots)} snapshots")

    print(f"Loading spike period: {args.spike_json}")
    spike_snapshots = load_snapshots(args.spike_json)
    print(f"  → Loaded {len(spike_snapshots)} snapshots")

    print("\nAnalyzing metrics...")
    normal_metrics = extract_metrics(normal_snapshots)
    spike_metrics = extract_metrics(spike_snapshots)

    print("Generating comparison report...")
    report = compare_metrics(normal_metrics, spike_metrics)

    with open(args.output, 'w') as f:
        f.write(report)

    print(f"\n✅ Report generated: {args.output}")
    print("\nPreview:")
    print("=" * 80)
    # Print first 50 lines
    lines = report.split('\n')
    for line in lines[:50]:
        print(line)
    if len(lines) > 50:
        print(f"\n... ({len(lines) - 50} more lines in full report)")


if __name__ == '__main__':
    main()
