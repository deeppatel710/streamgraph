#!/usr/bin/env python3
"""
CLI tool for analysts to confirm a component as fraud.

Sends a fraud-confirmation event to Kafka which the pipeline consumes to
write a Redis fraud label — all future transactions touching any entity in
this component will immediately score as CRITICAL (R09).

Usage
-----
    python scripts/confirm_fraud.py \\
        --component acct_001374 \\
        --members acct_001374 "ip:1.2.3.4" "card:9999" "merch_00295" \\
        --reason "confirmed chargeback — ring #42" \\
        --analyst alice@example.com

    # Or pipe a component snapshot JSON directly:
    echo '{"component_id":"acct_001374","member_ids":[...]}' | \\
        python scripts/confirm_fraud.py --from-snapshot -
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

import click


@click.command()
@click.option("--component", "-c", required=False, help="Component ID to confirm as fraud")
@click.option("--members", "-m", multiple=True, help="Entity IDs in the component")
@click.option("--reason", "-r", default="", help="Human-readable reason for confirmation")
@click.option("--analyst", "-a", default="analyst", help="Analyst identifier (email/name)")
@click.option("--bootstrap", "-b", default="localhost:9092", help="Kafka bootstrap servers")
@click.option("--topic", "-t", default="fraud-confirmations", help="Confirmation topic")
@click.option(
    "--from-snapshot", "from_snapshot", is_flag=True,
    help="Read a ComponentSnapshot JSON from stdin instead of specifying members",
)
def main(
    component: str | None,
    members: tuple[str, ...],
    reason: str,
    analyst: str,
    bootstrap: str,
    topic: str,
    from_snapshot: bool,
) -> None:
    """Confirm a fraud component and propagate labels via Kafka."""
    if from_snapshot:
        raw = sys.stdin.read().strip()
        snapshot = json.loads(raw)
        component = snapshot["component_id"]
        members = tuple(snapshot.get("member_ids", []))

    if not component:
        raise click.UsageError("--component is required (or use --from-snapshot)")

    event = {
        "component_id": component,
        "member_ids": list(members),
        "reason": reason,
        "analyst": analyst,
        "confirmed_at": datetime.now(tz=timezone.utc).isoformat(),
    }

    try:
        from kafka import KafkaProducer  # type: ignore[import]
    except ImportError:
        click.echo("kafka-python not installed: pip install kafka-python", err=True)
        sys.exit(1)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    future = producer.send(topic, event)
    producer.flush(timeout=10)
    metadata = future.get(timeout=10)

    click.echo(
        f"✓ Confirmation sent → topic={metadata.topic} "
        f"partition={metadata.partition} offset={metadata.offset}"
    )
    click.echo(f"  component_id : {component}")
    click.echo(f"  members      : {len(members)}")
    click.echo(f"  analyst      : {analyst}")
    click.echo(f"  reason       : {reason or '(none)'}")


if __name__ == "__main__":
    main()
