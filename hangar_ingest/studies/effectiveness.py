"""Maintenance effectiveness studies.

Each closed work order is aligned on its completion timestamp and surrounded by
symmetric pre/post observation windows.  Eligibility is decided per event, each
aircraft contributes at most one effect per metric, and the fleet summary is
computed over aircraft rather than over readings or events.  Every run freezes
the work-order revisions, calibration revisions and request parameters that
produced it; revisions are retained so corrections can be diffed later.
"""
import json
from decimal import Decimal, localcontext

from hangar_ingest.types import (
    canonical, decimal_string, digest, fail, from_micros, identity, instant, integer, micros, object_fields,
)
from hangar_ingest.runs import journal
from hangar_ingest.studies import fleet, statistics

SCHEMA = '''
CREATE TABLE IF NOT EXISTS effectiveness_studies(
 study_id TEXT PRIMARY KEY,revision INTEGER NOT NULL,request TEXT NOT NULL,request_digest TEXT NOT NULL,
 digest TEXT NOT NULL,created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS effectiveness_versions(
 study_id TEXT NOT NULL,revision INTEGER NOT NULL,payload TEXT NOT NULL,digest TEXT NOT NULL,
 created_at TEXT NOT NULL,PRIMARY KEY(study_id,revision));
'''

DEFAULTS = {
    'window_seconds': 86400,
    'resample_seconds': 3600,
    'minimum_buckets': 6,
    'minimum_coverage_ratio': '0.8',
    'minimum_aircraft': 5,
    'bootstrap_replicates': 2000,
    'confidence_level': '0.95',
    'bootstrap_seed': 20261111,
    'direction': 'lower_is_better',
}

REQUEST_OPTIONAL = {
    'aircraft', 'aircraft_types', 'order_types', 'components', 'metrics',
    'order_ids', 'start', 'end',
}
REQUEST_TUNABLE = set(DEFAULTS)
DIRECTIONS = {'lower_is_better', 'higher_is_better', 'undirected'}


class EffectivenessStore:
    """Idempotent schema bootstrap; safe to call inside an outer transaction.

    ``executescript`` issues an implicit COMMIT, which would break the atomic
    ``Application.batch`` boundary, so each DDL statement runs through execute.
    """

    def __init__(self, db):
        self.db = db
        for statement in (statement for statement in SCHEMA.split(';') if statement.strip()):
            db.execute(statement)


def _window_bounds(closed_at, window_seconds):
    anchor = micros(closed_at)
    span = window_seconds * 1000000
    return from_micros(anchor - span), from_micros(anchor), from_micros(anchor + span)


def _calibration_snapshot(db, device_id):
    rows = db.all(
        'SELECT calibration_id,revision,valid_from,valid_until,scale,offset,clock_offset_us '
        'FROM calibrations WHERE device_id=? ORDER BY valid_from,calibration_id',
        (device_id,),
    )
    return rows


def _calibration_complete(db, device_id, start, end, required):
    if not required:
        return True, []
    rows = db.all(
        'SELECT valid_from,valid_until,calibration_id FROM calibrations WHERE device_id=? ORDER BY valid_from',
        (device_id,),
    )
    lower, upper = micros(start), micros(end)
    cursor = lower
    gaps = []
    for row in rows:
        c_start, c_end = micros(row['valid_from']), micros(row['valid_until'])
        if c_end <= cursor:
            continue
        if c_start > cursor:
            gaps.append({'start': from_micros(cursor), 'end': from_micros(min(c_start, upper)), 'calibration_id': None})
        cursor = max(cursor, c_end)
        if cursor >= upper:
            break
    if cursor < upper:
        gaps.append({'start': from_micros(cursor), 'end': from_micros(upper), 'calibration_id': None})
    return not gaps, gaps


def _overlapping_orders(db, order, start, end):
    """Other non-cancelled orders on the same aircraft/component touching either window."""
    rows = db.all(
        "SELECT order_id,revision,opened_at,closed_at,state,order_type FROM work_orders "
        "WHERE aircraft=? AND component=? AND state!='cancelled' AND order_id!=? "
        'AND opened_at<? AND closed_at>? ORDER BY opened_at,order_id',
        (order['aircraft'], order['component'], order['order_id'], end, start),
    )
    return rows


def _coverage_ratio(buckets):
    if not buckets:
        return Decimal(0)
    occupied = sum(1 for bucket in buckets if bucket['occupied'])
    return Decimal(occupied) / Decimal(len(buckets))


def _normalize_request(db, parameters):
    object_fields(parameters, set(), REQUEST_OPTIONAL | REQUEST_TUNABLE)
    request = dict(DEFAULTS)
    for key in REQUEST_TUNABLE:
        if key in parameters:
            request[key] = parameters[key]
    integer(request['window_seconds'], 'window_seconds', 60, 90 * 86400)
    integer(request['resample_seconds'], 'resample_seconds', 1, request['window_seconds'])
    integer(request['minimum_buckets'], 'minimum_buckets', 1, 100000)
    integer(request['minimum_aircraft'], 'minimum_aircraft', 1, 100000)
    integer(request['bootstrap_replicates'], 'bootstrap_replicates', 10, 100000)
    integer(request['bootstrap_seed'], 'bootstrap_seed', 0, 2 ** 64 - 1)
    ratio = Decimal(str(request['minimum_coverage_ratio']))
    if not 0 < ratio <= 1:
        fail('coverage_ratio', 'Coverage ratio must be between zero and one')
    request['minimum_coverage_ratio'] = decimal_string(ratio)
    confidence = Decimal(str(request['confidence_level']))
    if not 0 < confidence < 1:
        fail('confidence_range', 'Confidence level must be strictly between zero and one')
    request['confidence_level'] = decimal_string(confidence)
    if request['direction'] not in DIRECTIONS:
        fail('direction', 'Unknown effect direction', allowed=sorted(DIRECTIONS))
    for key in ('aircraft',):
        if key in parameters and parameters[key] is not None:
            identity(parameters[key], key)
            request[key] = parameters[key]
    for list_key in ('aircraft_types', 'order_types', 'components', 'metrics', 'order_ids'):
        values = parameters.get(list_key)
        if values is None:
            continue
        if not isinstance(values, list) or not values or len(values) != len(set(values)) or len(values) > 1000:
            fail('filter_list', 'Filter must be a unique nonempty bounded list', field=list_key)
        request[list_key] = sorted(identity(str(value), list_key) for value in values)
    if 'start' in parameters and parameters['start'] is not None:
        request['start'] = instant(parameters['start'])
    if 'end' in parameters and parameters['end'] is not None:
        request['end'] = instant(parameters['end'])
    if request.get('start') and request.get('end') and request['start'] >= request['end']:
        fail('request_interval', 'Study interval must be nonempty')
    if request.get('aircraft_types'):
        known = {row['aircraft']: row['aircraft_type'] for row in fleet.listing(db)}
        unknown = [value for value in request['aircraft_types'] if value not in set(known.values())]
        if unknown:
            fail('unknown_aircraft_type', 'Aircraft type has no registered aircraft', aircraft_types=unknown)
    return request


def _candidate_orders(db, request):
    clauses = ["state='closed'"]
    args = []
    if request.get('aircraft'):
        clauses.append('aircraft=?')
        args.append(request['aircraft'])
    if request.get('components'):
        clauses.append(f"component IN ({','.join('?' for _ in request['components'])})")
        args.extend(request['components'])
    if request.get('order_types'):
        clauses.append(f"order_type IN ({','.join('?' for _ in request['order_types'])})")
        args.extend(request['order_types'])
    if request.get('order_ids'):
        clauses.append(f"order_id IN ({','.join('?' for _ in request['order_ids'])})")
        args.extend(request['order_ids'])
    if request.get('start'):
        clauses.append('closed_at>=?')
        args.append(request['start'])
    if request.get('end'):
        clauses.append('closed_at<?')
        args.append(request['end'])
    rows = db.all(
        'SELECT * FROM work_orders WHERE ' + ' AND '.join(clauses) + ' ORDER BY closed_at,order_id',
        args,
    )
    if request.get('aircraft_types'):
        allowed = {row['aircraft'] for row in fleet.listing(db) if row['aircraft_type'] in request['aircraft_types']}
        rows = [row for row in rows if row['aircraft'] in allowed]
    return rows


def _matching_devices(db, order, request):
    rows = db.all(
        'SELECT * FROM devices WHERE aircraft=? AND component=? ORDER BY device_id',
        (order['aircraft'], order['component']),
    )
    if request.get('metrics'):
        rows = [row for row in rows if row['metric'] in request['metrics']]
    return rows


def _evaluate_event(db, order, device, request):
    """Return (event_record, exclusion_record_or_None)."""
    pre_start, anchor, post_end = _window_bounds(order['closed_at'], request['window_seconds'])
    duration_us = micros(order['closed_at']) - micros(order['opened_at'])
    event_id = digest({'order_id': order['order_id'], 'device_id': device['device_id']})
    base = {
        'event_id': event_id,
        'order_id': order['order_id'],
        'order_revision': order['revision'],
        'aircraft': order['aircraft'],
        'component': order['component'],
        'device_id': device['device_id'],
        'metric': device['metric'],
        'unit': device['canonical_unit'],
        'order_type': order['order_type'],
        'anchor': anchor,
        'pre_window': {'start': pre_start, 'end': anchor},
        'post_window': {'start': anchor, 'end': post_end},
        'maintenance_duration_us': duration_us,
    }
    reasons = []

    def excluded(code, detail):
        reasons.append({'code': code, **detail})

    overlaps = _overlapping_orders(db, order, pre_start, post_end)
    if overlaps:
        excluded('overlapping_work_order', {
            'orders': [
                {'order_id': row['order_id'], 'revision': row['revision'], 'opened_at': row['opened_at'], 'closed_at': row['closed_at']}
                for row in overlaps
            ],
        })
    rule = db.one('SELECT required_calibration,max_gap_seconds FROM quality_rules WHERE device_id=?', (device['device_id'],))
    required_calibration = bool(rule['required_calibration']) if rule else True
    pre_cal_complete, pre_gaps = _calibration_complete(db, device['device_id'], pre_start, anchor, required_calibration)
    post_cal_complete, post_gaps = _calibration_complete(db, device['device_id'], anchor, post_end, required_calibration)
    if not pre_cal_complete:
        excluded('calibration_gap', {'window': 'pre', 'gaps': pre_gaps})
    if not post_cal_complete:
        excluded('calibration_gap', {'window': 'post', 'gaps': post_gaps})

    pre_buckets = statistics.resample_buckets(db, device['device_id'], pre_start, anchor, request['resample_seconds'])
    post_buckets = statistics.resample_buckets(db, device['device_id'], anchor, post_end, request['resample_seconds'])
    pre_coverage = _coverage_ratio(pre_buckets)
    post_coverage = _coverage_ratio(post_buckets)
    pre_occupied = sum(1 for bucket in pre_buckets if bucket['occupied'])
    post_occupied = sum(1 for bucket in post_buckets if bucket['occupied'])
    threshold = Decimal(request['minimum_coverage_ratio'])
    if pre_coverage < threshold:
        excluded('insufficient_coverage', {'window': 'pre', 'coverage_ratio': decimal_string(pre_coverage), 'required': request['minimum_coverage_ratio']})
    if post_coverage < threshold:
        excluded('insufficient_coverage', {'window': 'post', 'coverage_ratio': decimal_string(post_coverage), 'required': request['minimum_coverage_ratio']})
    if pre_occupied < request['minimum_buckets']:
        excluded('insufficient_samples', {'window': 'pre', 'occupied_buckets': pre_occupied, 'required_buckets': request['minimum_buckets']})
    if post_occupied < request['minimum_buckets']:
        excluded('insufficient_samples', {'window': 'post', 'occupied_buckets': post_occupied, 'required_buckets': request['minimum_buckets']})

    pre_level = statistics.level_value(pre_buckets)
    post_level = statistics.level_value(post_buckets)
    record = {
        **base,
        'eligible': not reasons,
        'exclusions': reasons,
        'pre': {
            'level': pre_level,
            'coverage_ratio': decimal_string(pre_coverage),
            'occupied_buckets': pre_occupied,
            'total_buckets': len(pre_buckets),
            'observation_ids': [oid for bucket in pre_buckets for oid in bucket['observation_ids']],
        },
        'post': {
            'level': post_level,
            'coverage_ratio': decimal_string(post_coverage),
            'occupied_buckets': post_occupied,
            'total_buckets': len(post_buckets),
            'observation_ids': [oid for bucket in post_buckets for oid in bucket['observation_ids']],
        },
        'calibrations': _calibration_snapshot(db, device['device_id']),
    }
    if record['eligible']:
        delta = Decimal(post_level) - Decimal(pre_level)
        relative = None
        if Decimal(pre_level) != 0:
            relative = delta / abs(Decimal(pre_level))
        record['delta'] = decimal_string(delta)
        record['relative_delta'] = decimal_string(relative) if relative is not None else None
        if request['direction'] == 'lower_is_better':
            improved = delta < 0
        elif request['direction'] == 'higher_is_better':
            improved = delta > 0
        else:
            improved = None
        record['improved'] = improved
    else:
        record['delta'] = None
        record['relative_delta'] = None
        record['improved'] = None
    exclusion = None
    if reasons:
        exclusion = {
            'event_id': event_id,
            'order_id': order['order_id'],
            'aircraft': order['aircraft'],
            'component': order['component'],
            'device_id': device['device_id'],
            'metric': device['metric'],
            'maintenance_duration_us': duration_us,
            'reasons': reasons,
        }
    return record, exclusion


def _aircraft_summaries(events):
    """One effect per (aircraft, metric): median of that aircraft's event deltas."""
    grouped = {}
    for event in events:
        if not event['eligible']:
            continue
        grouped.setdefault((event['aircraft'], event['metric']), []).append(event)
    result = []
    for (aircraft, metric), rows in sorted(grouped.items()):
        deltas = [Decimal(row['delta']) for row in rows]
        with localcontext() as ctx:
            ctx.prec = 28
            effect = statistics.median(deltas)
        improvements = [row['improved'] for row in rows if row['improved'] is not None]
        result.append({
            'aircraft': aircraft,
            'metric': metric,
            'unit': rows[0]['unit'],
            'events': len(rows),
            'event_ids': [row['event_id'] for row in rows],
            'effect': decimal_string(effect),
            'improved_events': sum(1 for value in improvements if value),
            'direction': 'undirected' if rows[0]['improved'] is None else 'directed',
        })
    return result


def _fleet_summary(aircraft_rows, request):
    by_metric = {}
    for row in aircraft_rows:
        by_metric.setdefault(row['metric'], []).append(row)
    summaries = []
    for metric in sorted(by_metric):
        rows = by_metric[metric]
        effects = {row['aircraft']: row['effect'] for row in rows}
        values = [Decimal(row['effect']) for row in rows]
        stats = statistics.summarize(values)
        descriptive = {
            'metric': metric,
            'unit': rows[0]['unit'],
            'aircraft': len(rows),
            'events': sum(row['events'] for row in rows),
            'effect_mean': stats['mean'],
            'effect_median': stats['median'],
            'effect_minimum': stats['minimum'],
            'effect_maximum': stats['maximum'],
            'improved_aircraft': sum(1 for row in rows if Decimal(row['effect']) < 0) if request['direction'] == 'lower_is_better'
            else sum(1 for row in rows if Decimal(row['effect']) > 0) if request['direction'] == 'higher_is_better' else None,
        }
        if len(rows) < request['minimum_aircraft']:
            summaries.append({
                **descriptive,
                'inference': 'descriptive_only',
                'reason': 'below_minimum_aircraft',
                'minimum_aircraft': request['minimum_aircraft'],
                'interval': None,
            })
            continue
        if len(rows) < 2:
            summaries.append({
                **descriptive,
                'inference': 'descriptive_only',
                'reason': 'cluster_bootstrap_requires_two_aircraft',
                'minimum_aircraft': request['minimum_aircraft'],
                'interval': None,
            })
            continue
        interval = statistics.cluster_bootstrap(
            effects,
            replicates=request['bootstrap_replicates'],
            confidence=request['confidence_level'],
            seed=request['bootstrap_seed'],
        )
        summaries.append({**descriptive, 'inference': 'bootstrap_interval', 'reason': None, 'interval': interval})
    return summaries


def build_payload(db, request):
    orders = _candidate_orders(db, request)
    events = []
    exclusions = []
    used_orders = []
    matched_candidates = 0
    for order in orders:
        devices_for_order = _matching_devices(db, order, request)
        if not devices_for_order:
            continue
        matched_candidates += 1
        used_orders.append({'order_id': order['order_id'], 'revision': order['revision']})
        for device in devices_for_order:
            event, exclusion = _evaluate_event(db, order, device, request)
            events.append(event)
            if exclusion:
                exclusions.append(exclusion)
    events.sort(key=lambda row: (row['aircraft'], row['anchor'], row['order_id'], row['device_id']))
    exclusions.sort(key=lambda row: (row['aircraft'], row['order_id'], row['device_id']))
    aircraft = _aircraft_summaries(events)
    fleet_summary = _fleet_summary(aircraft, request)
    calibration_keys = sorted({
        (entry['calibration_id'], entry['revision'])
        for event in events for entry in event['calibrations'] if entry['calibration_id']
    })
    payload = {
        'format': 'hangar-effectiveness-study',
        'version': 1,
        'request': request,
        'frozen_inputs': {
            'work_orders': used_orders,
            'calibrations': [{'calibration_id': cid, 'revision': rev} for cid, rev in calibration_keys],
            'aircraft_registry': [
                {'aircraft': row['aircraft'], 'aircraft_type': row['aircraft_type'], 'revision': row['revision']}
                for row in fleet.listing(db)
                if row['aircraft'] in sorted({event['aircraft'] for event in events})
            ],
        },
        'candidate_orders': matched_candidates,
        'matching_work_orders': len(orders),
        'events': events,
        'excluded_events': exclusions,
        'exclusion_counts': _count_reasons(exclusions),
        'aircraft_effects': aircraft,
        'fleet_summary': fleet_summary,
    }
    payload['digest'] = digest({key: value for key, value in payload.items() if key != 'digest'})
    return payload


def _count_reasons(exclusions):
    counts = {}
    for exclusion in exclusions:
        for reason in exclusion['reasons']:
            counts[reason['code']] = counts.get(reason['code'], 0) + 1
    return dict(sorted(counts.items()))


def run(db, study_id, **parameters):
    identity(study_id, 'study_id')
    EffectivenessStore(db)
    request = _normalize_request(db, parameters)
    request_digest = digest(request)
    with db.transaction():
        existing = db.one('SELECT * FROM effectiveness_studies WHERE study_id=?', (study_id,))
        payload = build_payload(db, request)
        if existing and existing['digest'] == payload['digest']:
            stored = db.one('SELECT payload FROM effectiveness_versions WHERE study_id=? AND revision=?', (study_id, existing['revision']))
            return {'status': 'unchanged', 'study_id': study_id, 'revision': existing['revision'], 'result': json.loads(stored['payload'])}
        next_revision = (existing['revision'] + 1) if existing else 1
        db.execute(
            'INSERT INTO effectiveness_versions(study_id,revision,payload,digest,created_at) VALUES(?,?,?,?,?)',
            (study_id, next_revision, canonical(payload), payload['digest'], db.clock()),
        )
        if existing:
            db.execute(
                'UPDATE effectiveness_studies SET revision=?,request=?,request_digest=?,digest=?,created_at=? WHERE study_id=?',
                (next_revision, canonical(request), request_digest, payload['digest'], db.clock(), study_id),
            )
        else:
            db.execute(
                'INSERT INTO effectiveness_studies(study_id,revision,request,request_digest,digest,created_at) VALUES(?,?,?,?,?,?)',
                (study_id, next_revision, canonical(request), request_digest, payload['digest'], db.clock()),
            )
        journal.record(db, 'effectiveness.run', {
            'study_id': study_id, 'revision': next_revision, 'digest': payload['digest'],
            'events': len(payload['events']), 'eligible': len(payload['aircraft_effects']),
        })
        return {'status': 'updated' if existing else 'created', 'study_id': study_id, 'revision': next_revision, 'result': payload}


def get(db, study_id, revision=None):
    identity(study_id, 'study_id')
    EffectivenessStore(db)
    header = db.one('SELECT * FROM effectiveness_studies WHERE study_id=?', (study_id,))
    if header is None:
        fail('unknown_study', 'Effectiveness study does not exist')
    wanted = header['revision'] if revision is None else integer(revision, 'revision', 1)
    row = db.one('SELECT * FROM effectiveness_versions WHERE study_id=? AND revision=?', (study_id, wanted))
    if row is None:
        fail('unknown_study_revision', 'Study revision does not exist')
    payload = json.loads(row['payload'])
    if digest({key: value for key, value in payload.items() if key != 'digest'}) != row['digest']:
        fail('study_corrupt', 'Stored study digest does not match content')
    return {'study_id': study_id, 'revision': wanted, 'current_revision': header['revision'], 'result': payload}


def listing(db):
    EffectivenessStore(db)
    rows = db.all('SELECT study_id,revision,digest,created_at FROM effectiveness_studies ORDER BY study_id')
    return rows


def revisions(db, study_id):
    identity(study_id, 'study_id')
    EffectivenessStore(db)
    if db.one('SELECT 1 FROM effectiveness_studies WHERE study_id=?', (study_id,)) is None:
        fail('unknown_study', 'Effectiveness study does not exist')
    return db.all(
        'SELECT study_id,revision,digest,created_at FROM effectiveness_versions WHERE study_id=? ORDER BY revision',
        (study_id,),
    )


REASON_MESSAGES = {
    'overlapping_work_order': '观察窗内存在其他未取消工单，维修影响无法与本次工单隔离',
    'calibration_gap': '观察窗存在校准覆盖缺口，读数无法统一到冻结校准版本',
    'insufficient_coverage': '重采样覆盖率低于研究要求，窗口内缺测过多',
    'insufficient_samples': '有效重采样桶数低于最少样本要求',
}


def explain_event(db, study_id, event_id, revision=None):
    identity(event_id, 'event_id')
    study = get(db, study_id, revision)
    payload = study['result']
    event = next((row for row in payload['events'] if row['event_id'] == event_id), None)
    exclusion = next((row for row in payload['excluded_events'] if row['event_id'] == event_id), None)
    if event is None:
        fail('unknown_event', 'Study does not contain this event', event_id=event_id)
    explanations = []
    for reason in event['exclusions']:
        explanations.append({
            'code': reason['code'],
            'message': REASON_MESSAGES.get(reason['code'], '事件不满足纳入条件'),
            'detail': {key: value for key, value in reason.items() if key != 'code'},
        })
    return {
        'study_id': study_id,
        'revision': study['revision'],
        'event': {key: event[key] for key in (
            'event_id', 'order_id', 'order_revision', 'aircraft', 'component', 'device_id',
            'metric', 'anchor', 'maintenance_duration_us', 'eligible', 'pre', 'post', 'delta', 'relative_delta', 'improved',
        )},
        'exclusion_record': exclusion,
        'explanations': explanations,
        'maintenance': {
            'opened_to_closed_us': event['maintenance_duration_us'],
            'note': '维修持续期不计入前窗；前窗结束于工单完成时刻，避免维修作业读数污染基线',
        },
    }


def compare(db, study_id, before, after):
    identity(study_id, 'study_id')
    integer(before, 'before', 1)
    integer(after, 'after', 1)
    left = get(db, study_id, before)['result']
    right = get(db, study_id, after)['result']
    old_events = {event['event_id']: event for event in left['events']}
    new_events = {event['event_id']: event for event in right['events']}
    changed_events = []
    for event_id in sorted(set(old_events) | set(new_events)):
        old = old_events.get(event_id)
        new = new_events.get(event_id)
        if old is None:
            kind = 'added'
        elif new is None:
            kind = 'removed'
        elif (
            old['eligible'] != new['eligible']
            or old.get('delta') != new.get('delta')
            or old.get('pre', {}).get('level') != new.get('pre', {}).get('level')
            or old.get('post', {}).get('level') != new.get('post', {}).get('level')
            or old['maintenance_duration_us'] != new['maintenance_duration_us']
            or old['anchor'] != new['anchor']
        ):
            kind = 'changed'
        else:
            continue
        changed_events.append({
            'event_id': event_id,
            'kind': kind,
            'order_id': (new or old)['order_id'],
            'before_eligible': None if old is None else old['eligible'],
            'after_eligible': None if new is None else new['eligible'],
            'before_delta': None if old is None else old.get('delta'),
            'after_delta': None if new is None else new.get('delta'),
            'before_maintenance_duration_us': None if old is None else old['maintenance_duration_us'],
            'after_maintenance_duration_us': None if new is None else new['maintenance_duration_us'],
            'before_exclusions': None if old is None else old['exclusions'],
            'after_exclusions': None if new is None else new['exclusions'],
        })
    old_orders = {entry['order_id']: entry for entry in left['frozen_inputs']['work_orders']}
    new_orders = {entry['order_id']: entry for entry in right['frozen_inputs']['work_orders']}
    changed_orders = []
    for order_id in sorted(set(old_orders) | set(new_orders)):
        if old_orders.get(order_id) != new_orders.get(order_id):
            changed_orders.append({'order_id': order_id, 'before': old_orders.get(order_id), 'after': new_orders.get(order_id)})
    old_cals = {(c['calibration_id'], c['revision']) for c in left['frozen_inputs']['calibrations']}
    new_cals = {(c['calibration_id'], c['revision']) for c in right['frozen_inputs']['calibrations']}
    return {
        'study_id': study_id,
        'before': before,
        'after': after,
        'request_changed': left['request'] != right['request'],
        'changed_events': changed_events,
        'changed_work_orders': changed_orders,
        'calibration_revisions_added': [
            {'calibration_id': cid, 'revision': rev} for cid, rev in sorted(new_cals - old_cals)
        ],
        'calibration_revisions_removed': [
            {'calibration_id': cid, 'revision': rev} for cid, rev in sorted(old_cals - new_cals)
        ],
        'fleet_summary_before': left['fleet_summary'],
        'fleet_summary_after': right['fleet_summary'],
    }
