"""Synthetic end-to-end demonstration for maintenance effectiveness studies.

The scenario is fully deterministic and uses only local data: five aircraft of
one registered type receive a component-replacement work order.  Four aircraft
have complete pre/post coverage; one aircraft has an overlapping work order and
is excluded with an explicit reason.  A calibration correction then produces a
second frozen revision and the revision diff identifies exactly which event
changed.
"""
import datetime as dt

from hangar_ingest.decoders import sensors
from hangar_ingest.quality import devices, recalibration, rules
from hangar_ingest.runs import batches, processor

ANCHOR = dt.datetime(2026, 6, 10, 12, tzinfo=dt.timezone.utc)
AIRCRAFT = ['plane1', 'plane2', 'plane3', 'plane4', 'plane5']

STUDY_PARAMETERS = {
    'metrics': ['vibration'],
    'window_seconds': 86400,
    'resample_seconds': 3600,
    'minimum_buckets': 6,
    'minimum_aircraft': 4,
    'bootstrap_replicates': 1000,
}


def _order(app, aircraft, *, order_id=None, opened_offset_hours=2, closed_offset_hours=0,
           state='closed', order_type='component_replacement', revision=1):
    app.call('order.upsert', {'value': {
        'order_id': order_id or f'wo-{aircraft}',
        'revision': revision,
        'aircraft': aircraft,
        'component': 'eng1',
        'opened_at': (ANCHOR - dt.timedelta(hours=opened_offset_hours)).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'closed_at': (ANCHOR - dt.timedelta(hours=closed_offset_hours)).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'issue': 'Synthetic high vibration',
        'resolution': 'Synthetic component replacement',
        'state': state,
        'order_type': order_type,
    }})


def _feed(db, index, device, *, missing=()):
    rows = []
    start = ANCHOR - dt.timedelta(hours=24)
    for hour in range(48):
        if hour in missing:
            continue
        level = 5.0 + index * 0.1 if hour < 24 else 2.0 + index * 0.05
        rows.append({
            'event_id': f'e{index}-{hour}',
            'device': device + '-ext',
            'observed_at': (start + dt.timedelta(hours=hour)).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'value': str(level),
            'unit': 'm/s2',
        })
    batches.stage(db, f'batch{index}', 'feed1', sensors.encode(rows))
    processor.drain(db, f'batch{index}')


def build(app, db):
    for index, aircraft in enumerate(AIRCRAFT, start=1):
        app.call('fleet.register', {'aircraft': aircraft, 'aircraft_type': 'A320'})
        device = f'{aircraft}-eng1-vib'
        devices.register(db, device, aircraft, 'eng1', 'vibration', 'm/s2')
        devices.add_alias(
            db, 'feed1', device + '-ext', device,
            '2026-01-01T00:00:00Z', '2027-01-01T00:00:00Z',
        )
        devices.add_calibration(
            db, f'cal-{aircraft}', device,
            '2026-01-01T00:00:00Z', '2027-01-01T00:00:00Z',
        )
        rules.configure(db, device, minimum='-1000', maximum='1000')
        _order(app, aircraft)
        _feed(db, index, device)

    # plane5 also has a conflicting order touching its observation windows.
    _order(app, 'plane5', order_id='wo-plane5-overlap', opened_offset_hours=3, closed_offset_hours=-1)

    first = app.call('effectiveness.run', {'study_id': 'demo-study', **STUDY_PARAMETERS})

    # Correct one calibration; observations are reprocessed and the study rerun.
    devices.correct_calibration(db, 'cal-plane2', 1, '1.1', '0', 0)
    recalibration.apply(db, 'cal-plane2', 2)
    second = app.call('effectiveness.run', {'study_id': 'demo-study', **STUDY_PARAMETERS})
    diff = app.call('effectiveness.compare', {'study_id': 'demo-study', 'before': 1, 'after': 2})
    return {
        'first_run': {'status': first['status'], 'revision': first['revision']},
        'second_run': {'status': second['status'], 'revision': second['revision']},
        'candidate_orders': second['result']['candidate_orders'],
        'eligible_aircraft': [row['aircraft'] for row in second['result']['aircraft_effects']],
        'excluded': [
            {'order_id': row['order_id'], 'reasons': [reason['code'] for reason in row['reasons']]}
            for row in second['result']['excluded_events']
        ],
        'fleet_summary': second['result']['fleet_summary'],
        'revision_diff': {
            'changed_events': [
                {'order_id': row['order_id'], 'kind': row['kind'],
                 'before_delta': row['before_delta'], 'after_delta': row['after_delta']}
                for row in diff['changed_events']
            ],
            'calibration_revisions_added': diff['calibration_revisions_added'],
        },
    }
