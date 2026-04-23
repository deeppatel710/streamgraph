"""
Synthetic fraud ring data generator for StreamGraph.

Generates a realistic mixture of:
  - Legitimate transactions (long-tail amount distribution, normal patterns)
  - Star rings      — one controller account funnels through many mule accounts
  - Chain rings     — linear layering: A→B→C→D→E
  - Cycle rings     — circular flow to obscure origin
  - Dense rings     — near-clique with many shared devices/IPs
  - Card-testing    — burst of micro-transactions from a single device

Usage
-----
  python -m generator.fraud_ring_generator --help
  python -m generator.fraud_ring_generator --events-per-second 500 --dry-run

Output: JSON lines to stdout *or* directly to a Kafka topic.
"""

from __future__ import annotations

import json
import logging
import random
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Generator, Iterator

import click
import numpy as np
from faker import Faker
from rich.console import Console
from rich.live import Live
from rich.table import Table

logger = logging.getLogger(__name__)
console = Console()
fake = Faker()
Faker.seed(42)

# Prometheus metrics — started lazily in main() when running against Kafka
try:
    from prometheus_client import Counter, Gauge, start_http_server  # type: ignore[import]
    _TXN_COUNTER = Counter(
        "streamgraph_transactions_generated_total",
        "Total transactions emitted by the generator",
    )
    _FRAUD_RATIO_GAUGE = Gauge(
        "streamgraph_fraud_ring_ratio",
        "Current fraction of emitted transactions from fraud rings",
    )
    _ACTIVE_RINGS_GAUGE = Gauge(
        "streamgraph_active_fraud_rings",
        "Number of active fraud ring definitions",
    )
    _PROM_AVAILABLE = True
except ImportError:
    _PROM_AVAILABLE = False


# ---------------------------------------------------------------------------
# Synthetic entity pools
# ---------------------------------------------------------------------------


@dataclass
class EntityPool:
    """Pre-generated entity IDs reused across transactions."""
    accounts: list[str]
    merchants: list[str]
    devices: list[str]
    ips: list[str]
    cards: list[str]
    mccs: list[str] = field(default_factory=lambda: [
        "5411", "5912", "5999", "4111", "7011",
        "5812", "5311", "6011", "4814", "7372",
    ])

    @classmethod
    def generate(
        cls,
        num_accounts: int = 10_000,
        num_merchants: int = 2_000,
        seed: int = 42,
    ) -> "EntityPool":
        rng = random.Random(seed)
        np_rng = np.random.default_rng(seed)

        accounts = [f"acct_{i:06d}" for i in range(num_accounts)]
        merchants = [f"merch_{i:05d}" for i in range(num_merchants)]
        devices = [str(uuid.UUID(int=rng.getrandbits(128))) for _ in range(num_accounts // 3)]
        ips = [fake.ipv4_public() for _ in range(num_accounts // 5)]
        cards = [f"{np_rng.integers(1000, 9999)}" for _ in range(num_accounts)]
        return cls(accounts=accounts, merchants=merchants,
                   devices=devices, ips=ips, cards=cards)


# ---------------------------------------------------------------------------
# Fraud ring topologies
# ---------------------------------------------------------------------------


@dataclass
class FraudRing:
    ring_id: str
    pattern: str
    members: list[str]              # account IDs
    shared_devices: list[str]
    shared_ips: list[str]
    merchants: list[str]            # merchants used by ring
    amount_range: tuple[float, float]

    def __len__(self) -> int:
        return len(self.members)


def _make_star_ring(
    ring_id: str,
    pool: EntityPool,
    size: int,
    rng: random.Random,
) -> FraudRing:
    """One hub account connected to many spokes (mule accounts)."""
    members = rng.sample(pool.accounts, k=size)
    shared_devices = rng.sample(pool.devices, k=max(1, size // 4))
    shared_ips = rng.sample(pool.ips, k=max(1, size // 6))
    merchants = rng.sample(pool.merchants, k=max(2, size // 3))
    return FraudRing(
        ring_id=ring_id, pattern="star",
        members=members, shared_devices=shared_devices,
        shared_ips=shared_ips, merchants=merchants,
        amount_range=(50.0, 900.0),
    )


def _make_chain_ring(
    ring_id: str,
    pool: EntityPool,
    size: int,
    rng: random.Random,
) -> FraudRing:
    """Layering: A → B → C → … → N, each pair shares at least one device."""
    members = rng.sample(pool.accounts, k=size)
    shared_devices = rng.sample(pool.devices, k=max(1, size // 5))
    shared_ips = rng.sample(pool.ips, k=max(1, size // 8))
    merchants = rng.sample(pool.merchants, k=2)
    return FraudRing(
        ring_id=ring_id, pattern="chain",
        members=members, shared_devices=shared_devices,
        shared_ips=shared_ips, merchants=merchants,
        amount_range=(500.0, 5_000.0),
    )


def _make_cycle_ring(
    ring_id: str,
    pool: EntityPool,
    size: int,
    rng: random.Random,
) -> FraudRing:
    members = rng.sample(pool.accounts, k=size)
    shared_devices = rng.sample(pool.devices, k=max(1, size // 3))
    shared_ips = rng.sample(pool.ips, k=max(1, size // 4))
    merchants = rng.sample(pool.merchants, k=max(3, size // 2))
    return FraudRing(
        ring_id=ring_id, pattern="cycle",
        members=members, shared_devices=shared_devices,
        shared_ips=shared_ips, merchants=merchants,
        amount_range=(100.0, 2_000.0),
    )


def _make_dense_ring(
    ring_id: str,
    pool: EntityPool,
    size: int,
    rng: random.Random,
) -> FraudRing:
    """Near-clique: many accounts share the same device/IP fingerprint."""
    size = min(size, 12)    # dense rings are small but tightly connected
    members = rng.sample(pool.accounts, k=size)
    shared_devices = rng.sample(pool.devices, k=2)
    shared_ips = rng.sample(pool.ips, k=1)
    merchants = rng.sample(pool.merchants, k=3)
    return FraudRing(
        ring_id=ring_id, pattern="dense",
        members=members, shared_devices=shared_devices,
        shared_ips=shared_ips, merchants=merchants,
        amount_range=(1.0, 50.0),    # card testing amounts
    )


RING_FACTORIES = {
    "star":  _make_star_ring,
    "chain": _make_chain_ring,
    "cycle": _make_cycle_ring,
    "dense": _make_dense_ring,
}


def generate_fraud_rings(
    pool: EntityPool,
    num_rings: int = 20,
    min_size: int = 4,
    max_size: int = 25,
    seed: int = 42,
) -> list[FraudRing]:
    rng = random.Random(seed)
    rings: list[FraudRing] = []
    patterns = list(RING_FACTORIES.keys())
    for i in range(num_rings):
        pattern = rng.choice(patterns)
        size = rng.randint(min_size, max_size)
        factory = RING_FACTORIES[pattern]
        ring = factory(f"ring_{i:04d}", pool, size, rng)
        rings.append(ring)
    return rings


# ---------------------------------------------------------------------------
# Transaction generation
# ---------------------------------------------------------------------------


def _random_amount(
    mean: float,
    std: float,
    lo: float,
    hi: float,
    rng: np.random.Generator,
) -> float:
    v = rng.lognormal(mean=np.log(mean), sigma=std / mean)
    return float(np.clip(v, lo, hi))


def _now_jitter(seconds_jitter: float, rng: random.Random) -> datetime:
    delta = rng.uniform(-seconds_jitter, 0)
    return datetime.utcnow() + timedelta(seconds=delta)


def _txn(
    account_id: str,
    merchant_id: str,
    amount: float,
    device_id: str | None,
    ip_address: str | None,
    card_last4: str | None,
    timestamp: datetime,
    mcc: str | None = None,
    country_code: str | None = None,
    metadata: dict | None = None,
) -> dict:
    return {
        "transaction_id": str(uuid.uuid4()),
        "timestamp": timestamp.isoformat(),
        "account_id": account_id,
        "merchant_id": merchant_id,
        "amount_usd": round(amount, 2),
        "currency": "USD",
        "status": "pending",
        "device_id": device_id,
        "ip_address": ip_address,
        "card_last4": card_last4,
        "mcc": mcc,
        "country_code": country_code,
        "metadata": metadata or {},
    }


def _legitimate_transactions(
    pool: EntityPool,
    count: int,
    rng: random.Random,
    np_rng: np.random.Generator,
) -> Generator[dict, None, None]:
    for _ in range(count):
        account = rng.choice(pool.accounts)
        merchant = rng.choice(pool.merchants)
        # Most legit users have a single device
        device = rng.choice(pool.devices) if rng.random() > 0.05 else None
        ip = rng.choice(pool.ips) if rng.random() > 0.1 else None
        card = rng.choice(pool.cards) if rng.random() > 0.02 else None
        amount = _random_amount(85.0, 120.0, 0.50, 5_000.0, np_rng)
        ts = _now_jitter(300, rng)
        yield _txn(account, merchant, amount, device, ip, card, ts,
                   mcc=rng.choice(pool.mccs),
                   country_code="US")


def _ring_transactions(
    ring: FraudRing,
    count_per_ring: int,
    rng: random.Random,
    np_rng: np.random.Generator,
) -> Generator[dict, None, None]:
    members = ring.members
    merchants = ring.merchants
    devices = ring.shared_devices
    ips = ring.shared_ips

    if ring.pattern == "star":
        hub = members[0]
        spokes = members[1:]
        for _ in range(count_per_ring):
            spoke = rng.choice(spokes)
            # Hub initiates, spoke receives (via merchant proxy)
            amount = _random_amount(
                (ring.amount_range[0] + ring.amount_range[1]) / 2,
                ring.amount_range[1] - ring.amount_range[0],
                ring.amount_range[0],
                ring.amount_range[1],
                np_rng,
            )
            ts = _now_jitter(120, rng)
            yield _txn(
                hub, rng.choice(merchants), amount,
                rng.choice(devices), rng.choice(ips),
                None, ts,
                metadata={"ring_id": ring.ring_id, "pattern": "star", "role": "hub"},
            )
            yield _txn(
                spoke, rng.choice(merchants), amount * rng.uniform(0.8, 1.0),
                rng.choice(devices), rng.choice(ips),
                None, ts + timedelta(seconds=rng.randint(5, 120)),
                metadata={"ring_id": ring.ring_id, "pattern": "star", "role": "spoke"},
            )

    elif ring.pattern == "chain":
        for _ in range(count_per_ring):
            for i in range(len(members) - 1):
                src = members[i]
                dst = members[i + 1]
                amount = _random_amount(
                    (ring.amount_range[0] + ring.amount_range[1]) / 2,
                    200.0, ring.amount_range[0], ring.amount_range[1], np_rng,
                )
                ts = _now_jitter(600, rng) + timedelta(minutes=i * 5)
                # Shared device between consecutive hops is the ring signal
                shared_dev = devices[i % len(devices)]
                yield _txn(
                    src, rng.choice(merchants), amount,
                    shared_dev, rng.choice(ips), None, ts,
                    metadata={"ring_id": ring.ring_id, "pattern": "chain", "hop": i},
                )

    elif ring.pattern == "cycle":
        for _ in range(count_per_ring):
            for i, acc in enumerate(members):
                next_acc = members[(i + 1) % len(members)]
                amount = _random_amount(
                    500.0, 300.0,
                    ring.amount_range[0], ring.amount_range[1], np_rng,
                )
                ts = _now_jitter(300, rng) + timedelta(minutes=i * 3)
                yield _txn(
                    acc, rng.choice(merchants), amount,
                    rng.choice(devices), rng.choice(ips), None, ts,
                    metadata={"ring_id": ring.ring_id, "pattern": "cycle", "pos": i},
                )

    elif ring.pattern == "dense":
        # Card testing: rapid small transactions from all members on same device/IP
        for _ in range(count_per_ring):
            for acc in members:
                amount = round(rng.uniform(0.01, 2.0), 2)   # micro-amounts
                ts = _now_jitter(60, rng)
                yield _txn(
                    acc, rng.choice(merchants), amount,
                    devices[0], ips[0], None, ts,
                    metadata={"ring_id": ring.ring_id, "pattern": "dense"},
                )


# ---------------------------------------------------------------------------
# Streaming emitter
# ---------------------------------------------------------------------------


class TransactionEmitter:
    """
    Emits a continuous stream of transactions at a target rate.

    Either writes JSON lines to a file-like object or publishes to Kafka.
    """

    def __init__(
        self,
        pool: EntityPool,
        rings: list[FraudRing],
        events_per_second: float = 200.0,
        fraud_ring_ratio: float = 0.08,
        seed: int = 42,
    ) -> None:
        self._pool = pool
        self._rings = rings
        self._eps = events_per_second
        self._fraud_ratio = fraud_ring_ratio
        self._rng = random.Random(seed)
        self._np_rng = np.random.default_rng(seed)
        self._emitted = 0
        self._fraud_emitted = 0

    def _next_batch(self, batch_size: int = 100) -> list[dict]:
        txns: list[dict] = []
        fraud_count = int(batch_size * self._fraud_ratio)
        legit_count = batch_size - fraud_count

        # Legitimate
        txns.extend(
            list(_legitimate_transactions(self._pool, legit_count, self._rng, self._np_rng))
        )
        # Fraud ring
        if fraud_count > 0 and self._rings:
            ring = self._rng.choice(self._rings)
            count_per_ring = max(1, fraud_count // max(1, len(ring.members)))
            fraud_txns = list(
                _ring_transactions(ring, count_per_ring, self._rng, self._np_rng)
            )
            txns.extend(fraud_txns[:fraud_count])
            self._fraud_emitted += min(len(fraud_txns), fraud_count)

        self._rng.shuffle(txns)
        self._emitted += len(txns)
        if _PROM_AVAILABLE:
            _TXN_COUNTER.inc(len(txns))
            ratio = self._fraud_emitted / self._emitted if self._emitted else 0.0
            _FRAUD_RATIO_GAUGE.set(ratio)
        return txns

    def stream_to_stdout(self) -> None:
        batch_size = max(10, int(self._eps))
        sleep_interval = batch_size / self._eps

        while True:
            t0 = time.monotonic()
            batch = self._next_batch(batch_size)
            for txn in batch:
                sys.stdout.write(json.dumps(txn) + "\n")
            sys.stdout.flush()
            elapsed = time.monotonic() - t0
            remaining = sleep_interval - elapsed
            if remaining > 0:
                time.sleep(remaining)

    def stream_to_kafka(self, producer: Any, topic: str) -> Iterator[int]:
        """
        Generator — yields emitted count after each batch.
        Caller controls the loop so it can implement graceful shutdown.
        """
        batch_size = max(10, int(self._eps))
        sleep_interval = batch_size / self._eps

        while True:
            t0 = time.monotonic()
            batch = self._next_batch(batch_size)
            for txn in batch:
                producer.send(topic, value=json.dumps(txn).encode("utf-8"))
            producer.flush()
            elapsed = time.monotonic() - t0
            remaining = sleep_interval - elapsed
            if remaining > 0:
                time.sleep(remaining)
            yield self._emitted


from typing import Any   # noqa: E402 (already imported above, but needed for type hint)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@click.command()
@click.option("--kafka-bootstrap", default="localhost:9092", show_default=True)
@click.option("--topic", default="raw-transactions", show_default=True)
@click.option("--events-per-second", default=200.0, type=float, show_default=True)
@click.option("--fraud-ratio", default=0.08, type=float, show_default=True)
@click.option("--num-accounts", default=10_000, type=int, show_default=True)
@click.option("--num-merchants", default=2_000, type=int, show_default=True)
@click.option("--num-rings", default=20, type=int, show_default=True)
@click.option("--ring-min-size", default=4, type=int, show_default=True)
@click.option("--ring-max-size", default=25, type=int, show_default=True)
@click.option("--seed", default=42, type=int, show_default=True)
@click.option("--dry-run", is_flag=True, default=False,
              help="Print first 20 transactions and exit")
@click.option("--stdout", "use_stdout", is_flag=True, default=False,
              help="Write to stdout instead of Kafka")
def main(
    kafka_bootstrap: str,
    topic: str,
    events_per_second: float,
    fraud_ratio: float,
    num_accounts: int,
    num_merchants: int,
    num_rings: int,
    ring_min_size: int,
    ring_max_size: int,
    seed: int,
    dry_run: bool,
    use_stdout: bool,
) -> None:
    """Generate synthetic fraud ring transactions and emit to Kafka or stdout."""
    console.print("[bold green]StreamGraph Data Generator[/]")
    console.print(f"  accounts={num_accounts:,}  merchants={num_merchants:,}")
    console.print(f"  rings={num_rings}  fraud_ratio={fraud_ratio:.1%}")
    console.print(f"  target rate={events_per_second:.0f} events/s")

    pool = EntityPool.generate(num_accounts, num_merchants, seed)
    rings = generate_fraud_rings(pool, num_rings, ring_min_size, ring_max_size, seed)

    console.print(f"\nGenerated {len(rings)} fraud rings:")
    pattern_counts: dict[str, int] = {}
    for r in rings:
        pattern_counts[r.pattern] = pattern_counts.get(r.pattern, 0) + 1
    for pat, cnt in pattern_counts.items():
        console.print(f"  {pat}: {cnt}")

    emitter = TransactionEmitter(pool, rings, events_per_second, fraud_ratio, seed)

    if _PROM_AVAILABLE:
        _ACTIVE_RINGS_GAUGE.set(len(rings))
        start_http_server(9090)
        console.print("  metrics exposed on :9090")

    if dry_run:
        rng = random.Random(seed)
        np_rng = np.random.default_rng(seed)
        sample = list(_legitimate_transactions(pool, 10, rng, np_rng))
        sample += list(_ring_transactions(rings[0], 1, rng, np_rng))[:10]
        for t in sample[:20]:
            console.print(json.dumps(t, indent=2))
        return

    if use_stdout:
        emitter.stream_to_stdout()
        return

    try:
        from kafka import KafkaProducer  # type: ignore[import]
    except ImportError:
        console.print("[red]kafka-python not installed. Use --stdout instead.[/]")
        raise SystemExit(1)

    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        batch_size=65536,
        linger_ms=5,
        compression_type="lz4",
    )
    console.print(f"\n[bold]Streaming to Kafka[/] bootstrap={kafka_bootstrap} topic={topic}")

    table = Table(title="Emission Stats", min_width=40)
    table.add_column("Total emitted")
    table.add_column("Fraud emitted")
    table.add_column("Fraud %")

    with Live(table, refresh_per_second=2, console=console):
        for total in emitter.stream_to_kafka(producer, topic):
            table = Table(title="Emission Stats", min_width=40)
            table.add_column("Total emitted")
            table.add_column("Fraud emitted")
            table.add_column("Fraud %")
            ratio = (emitter._fraud_emitted / total * 100) if total else 0
            table.add_row(
                f"{total:,}",
                f"{emitter._fraud_emitted:,}",
                f"{ratio:.1f}%",
            )


if __name__ == "__main__":
    main()
