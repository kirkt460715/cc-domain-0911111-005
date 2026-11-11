"""Deterministic statistics used by effectiveness studies.

Only the Python standard library is used. Randomness is a seeded LCG so that
frozen studies can be rebuilt byte-for-byte without relying on process state.
"""
from decimal import Decimal, localcontext

from hangar_ingest.types import decimal_string, fail, from_micros, integer, micros
from hangar_ingest.runs.windows import select, statistics


class DeterministicRng:
    """64-bit LCG (Knuth MMIX constants); enough for ordered resampling draws."""

    MODULUS = 1 << 64
    MULTIPLIER = 6364136223846793005
    INCREMENT = 1442695040888963407

    def __init__(self, seed):
        integer(seed, 'seed', 0, self.MODULUS - 1)
        self.state = seed

    def next_index(self, length):
        if length <= 0:
            fail('resample_empty', 'Cannot draw from an empty sample')
        self.state = (self.MULTIPLIER * self.state + self.INCREMENT) % self.MODULUS
        return self.state % length


def mean(values):
    if not values:
        return None
    with localcontext() as ctx:
        ctx.prec = 28
        return sum(values, Decimal(0)) / len(values)


def median(values):
    ordered = sorted(values)
    if not ordered:
        return None
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


def summarize(values):
    decimals = [Decimal(value) for value in values]
    if not decimals:
        return {'count': 0, 'mean': None, 'median': None, 'minimum': None, 'maximum': None}
    return {
        'count': len(decimals),
        'mean': decimal_string(mean(decimals)),
        'median': decimal_string(median(decimals)),
        'minimum': decimal_string(min(decimals)),
        'maximum': decimal_string(max(decimals)),
    }


def resample_buckets(db, device_id, start, end, interval_seconds):
    """Evenly weighted time buckets; each nonempty bucket contributes one median."""
    integer(interval_seconds, 'interval_seconds', 1, 86400)
    start_us = micros(start)
    end_us = micros(end)
    if start_us >= end_us:
        fail('window_interval', 'Observation window must be nonempty')
    bucket_size = interval_seconds * 1000000
    if (end_us - start_us) // bucket_size > 100000:
        fail('bucket_limit', 'Resampling would create too many buckets')
    buckets = {}
    raw = select(db, device_id, start, end)
    for row in raw:
        index = (micros(row['event_time']) - start_us) // bucket_size
        buckets.setdefault(index, []).append(row)
    result = []
    for index, lower in enumerate(range(start_us, end_us, bucket_size)):
        rows = buckets.get(index, [])
        stats = statistics(rows)
        result.append({
            'start': from_micros(lower),
            'end': from_micros(min(lower + bucket_size, end_us)),
            'occupied': bool(rows),
            'samples': stats['count'],
            'median': stats['median'],
            'mean': stats['mean'],
            'observation_ids': [row['observation_id'] for row in rows],
        })
    return result


def level_value(buckets):
    """Representative level: median of the occupied bucket medians."""
    occupied = [Decimal(bucket['median']) for bucket in buckets if bucket['occupied']]
    if not occupied:
        return None
    return decimal_string(median(occupied))


def percentile_fraction(values, fraction):
    ordered = sorted(values)
    if not ordered:
        return None
    fraction = Decimal(str(fraction))
    position = fraction * (len(ordered) - 1)
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    share = position - lower
    return ordered[lower] + share * (ordered[upper] - ordered[lower])


def cluster_bootstrap(aircraft_effects, *, replicates, confidence, seed):
    """Resample whole aircraft, never individual events.

    aircraft_effects maps aircraft identifier to its single per-aircraft effect.
    Returns percentile confidence intervals for the fleet mean and median.
    """
    integer(replicates, 'replicates', 10, 100000)
    confidence = Decimal(str(confidence))
    if not 0 < confidence < 1:
        fail('confidence_range', 'Confidence level must be strictly between zero and one')
    labels = sorted(aircraft_effects)
    effects = [Decimal(aircraft_effects[label]) for label in labels]
    if len(labels) < 2:
        fail('bootstrap_aircraft', 'Cluster bootstrap needs at least two aircraft')
    rng = DeterministicRng(seed)
    means = []
    medians = []
    for _ in range(replicates):
        drawn = [effects[rng.next_index(len(effects))] for _ in effects]
        means.append(mean(drawn))
        medians.append(median(drawn))
    tail = (1 - confidence) / 2
    return {
        'method': 'aircraft_cluster_bootstrap_percentile',
        'replicates': replicates,
        'confidence_level': decimal_string(Decimal(str(confidence))),
        'seed': seed,
        'aircraft_per_replicate': len(effects),
        'mean': {
            'lower': decimal_string(percentile_fraction(means, tail)),
            'upper': decimal_string(percentile_fraction(means, 1 - tail)),
        },
        'median': {
            'lower': decimal_string(percentile_fraction(medians, tail)),
            'upper': decimal_string(percentile_fraction(medians, 1 - tail)),
        },
    }
