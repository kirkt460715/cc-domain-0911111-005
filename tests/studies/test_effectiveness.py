import datetime as dt
import re
import sqlite3
import tempfile
import unittest
from pathlib import Path

from hangar_ingest.api import Application
from hangar_ingest.decoders import sensors
from hangar_ingest.quality import devices, recalibration, rules
from hangar_ingest.runs import batches, processor
from hangar_ingest.runs.database import Database
from hangar_ingest.studies import effectiveness, statistics
from hangar_ingest.types import PipelineError, digest
from tests.support import Clock

ANCHOR = dt.datetime(2026, 6, 10, 12, tzinfo=dt.timezone.utc)


class StudyCase(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.clock = Clock()
        self.db = Database(Path(self.directory.name) / 'studies.db', clock=self.clock)
        self.app = Application(self.db)

    def tearDown(self):
        self.db.close()
        self.directory.cleanup()

    def register_aircraft(self, index, aircraft_type='A320', component='eng1'):
        aircraft = f'plane{index}'
        device = f'{aircraft}-{component}-vib'
        self.app.call('fleet.register', {'aircraft': aircraft, 'aircraft_type': aircraft_type})
        devices.register(self.db, device, aircraft, component, 'vibration', 'm/s2')
        devices.add_alias(
            self.db, 'feed1', device + '-ext', device,
            '2026-01-01T00:00:00Z', '2027-01-01T00:00:00Z',
        )
        devices.add_calibration(
            self.db, f'cal-{aircraft}', device,
            '2026-01-01T00:00:00Z', '2027-01-01T00:00:00Z',
        )
        rules.configure(self.db, device, minimum='-1000', maximum='1000')
        return aircraft, device

    def work_order(self, aircraft, *, order_id=None, opened_hours=2, closed_hours=0,
                   component='eng1', order_type='component_replacement', state='closed', revision=1):
        order_id = order_id or f'wo-{aircraft}'
        opened = (ANCHOR - dt.timedelta(hours=opened_hours)).strftime('%Y-%m-%dT%H:%M:%SZ')
        closed = (ANCHOR - dt.timedelta(hours=closed_hours)).strftime('%Y-%m-%dT%H:%M:%SZ')
        self.app.call('order.upsert', {'value': {
            'order_id': order_id, 'revision': revision, 'aircraft': aircraft, 'component': component,
            'opened_at': opened, 'closed_at': closed, 'issue': 'high vibration',
            'resolution': 'component replaced', 'state': state, 'order_type': order_type,
        }})
        return order_id

    def feed(self, device, index, *, pre=5.0, post=2.0, missing_hours=(), hours=48):
        rows = []
        for hour in range(hours):
            if hour in missing_hours:
                continue
            stamp = (ANCHOR - dt.timedelta(hours=24) + dt.timedelta(hours=hour)).strftime('%Y-%m-%dT%H:%M:%SZ')
            level = pre if hour < 24 else post
            rows.append({
                'event_id': f'e{index}-{hour}', 'device': device + '-ext',
                'observed_at': stamp, 'value': str(level), 'unit': 'm/s2',
            })
        batches.stage(self.db, f'batch{index}', 'feed1', sensors.encode(rows))
        processor.drain(self.db, f'batch{index}')

    def fleet(self, count=5, **feed_options):
        for index in range(1, count + 1):
            aircraft, device = self.register_aircraft(index)
            self.work_order(aircraft)
            self.feed(device, index, **feed_options)

    def params(self, **overrides):
        values = {
            'metrics': ['vibration'], 'window_seconds': 86400, 'resample_seconds': 3600,
            'minimum_buckets': 6, 'minimum_aircraft': 5, 'bootstrap_replicates': 500,
        }
        values.update(overrides)
        return values


class FleetRegistryTests(StudyCase):
    def test_registry_requires_revision_advance_and_lists_types(self):
        self.app.call('fleet.register', {'aircraft': 'plane1', 'aircraft_type': 'A320'})
        row = self.app.call('fleet.get', {'aircraft': 'plane1'})
        self.assertEqual(row['revision'], 1)
        with self.assertRaises(PipelineError) as caught:
            self.app.call('fleet.register', {'aircraft': 'plane1', 'aircraft_type': 'B737', 'revision': 5})
        self.assertEqual(caught.exception.code, 'stale_revision')
        updated = self.app.call('fleet.register', {'aircraft': 'plane1', 'aircraft_type': 'B737', 'revision': 1})
        self.assertEqual(updated['revision'], 2)
        self.assertEqual(self.app.call('fleet.list', {'aircraft_type': 'B737'})[0]['aircraft'], 'plane1')
        with self.assertRaises(PipelineError):
            self.app.call('fleet.get', {'aircraft': 'missing'})


class StudyEligibilityTests(StudyCase):
    def test_windows_anchor_on_completion_and_maintenance_duration_is_excluded(self):
        self.fleet(count=2)
        result = self.app.call('effectiveness.run', {'study_id': 's1', **self.params(minimum_aircraft=2)})['result']
        event = result['events'][0]
        self.assertEqual(event['anchor'], '2026-06-10T12:00:00.000000Z')
        self.assertEqual(event['pre_window']['end'], event['anchor'])
        self.assertEqual(event['post_window']['start'], event['anchor'])
        self.assertEqual(event['maintenance_duration_us'], 2 * 3600 * 1000000)
        self.assertTrue(event['eligible'])
        self.assertEqual(event['pre']['occupied_buckets'], 24)
        self.assertEqual(event['post']['occupied_buckets'], 24)

    def test_dense_aircraft_counts_once_through_aircraft_effect(self):
        # Two repairs per aircraft; the second completion is four days after the
        # first so the 24h windows never touch the other work order.
        for index in (1, 2):
            aircraft, device = self.register_aircraft(index)
            self.work_order(aircraft, order_id=f'wo-{aircraft}-a')
            self.app.call('order.upsert', {'value': {
                'order_id': f'wo-{aircraft}-b', 'revision': 1, 'aircraft': aircraft, 'component': 'eng1',
                'opened_at': (ANCHOR + dt.timedelta(hours=94)).strftime('%Y-%m-%dT%H:%M:%SZ'),
                'closed_at': (ANCHOR + dt.timedelta(hours=96)).strftime('%Y-%m-%dT%H:%M:%SZ'),
                'issue': 'high vibration', 'resolution': 'component replaced',
                'state': 'closed', 'order_type': 'component_replacement',
            }})
            rows = []
            start = ANCHOR - dt.timedelta(hours=24)
            for hour in range(168):
                stamp = (start + dt.timedelta(hours=hour)).strftime('%Y-%m-%dT%H:%M:%SZ')
                high = hour < 24 or 120 <= hour < 144
                rows.append({
                    'event_id': f'e{index}-{hour}', 'device': device + '-ext',
                    'observed_at': stamp, 'value': '5.0' if high else '2.0', 'unit': 'm/s2',
                })
            batches.stage(self.db, f'batch{index}', 'feed1', sensors.encode(rows))
            processor.drain(self.db, f'batch{index}')
        result = self.app.call('effectiveness.run', {'study_id': 's1', **self.params(minimum_aircraft=2)})['result']
        self.assertEqual(len(result['events']), 4)
        self.assertTrue(all(event['eligible'] for event in result['events']))
        aircraft_effects = result['aircraft_effects']
        self.assertEqual([row['aircraft'] for row in aircraft_effects], ['plane1', 'plane2'])
        self.assertEqual({row['events'] for row in aircraft_effects}, {2})
        summary = result['fleet_summary'][0]
        self.assertEqual(summary['aircraft'], 2)
        self.assertEqual(summary['events'], 4)

    def test_overlapping_work_order_is_excluded_with_detail(self):
        self.fleet(count=3)
        self.work_order('plane1', order_id='wo-overlap', opened_hours=3, closed_hours=-1)
        result = self.app.call('effectiveness.run', {'study_id': 's1', **self.params(minimum_aircraft=2)})['result']
        event = next(row for row in result['events'] if row['order_id'] == 'wo-plane1')
        self.assertFalse(event['eligible'])
        self.assertEqual([reason['code'] for reason in event['exclusions']], ['overlapping_work_order'])
        self.assertEqual(event['exclusions'][0]['orders'][0]['order_id'], 'wo-overlap')
        self.assertEqual(result['exclusion_counts']['overlapping_work_order'], 2)
        explanation = self.app.call('effectiveness.explain', {'study_id': 's1', 'event_id': event['event_id']})
        self.assertIn('隔离', explanation['explanations'][0]['message'])

    def test_missing_observations_fail_coverage_and_sample_thresholds(self):
        aircraft, device = self.register_aircraft(1)
        self.work_order(aircraft)
        self.feed(device, 1, missing_hours=set(range(24)))
        result = self.app.call('effectiveness.run', {'study_id': 's1', **self.params(minimum_aircraft=1)})['result']
        event = result['events'][0]
        self.assertFalse(event['eligible'])
        codes = {reason['code'] for reason in event['exclusions']}
        self.assertIn('insufficient_coverage', codes)
        self.assertIn('insufficient_samples', codes)
        reason = next(row for row in event['exclusions'] if row['code'] == 'insufficient_coverage')
        self.assertEqual(reason['window'], 'pre')

    def test_calibration_gap_excludes_event(self):
        aircraft, device = self.register_aircraft(1)
        self.db.execute('DELETE FROM calibrations')
        devices.add_calibration(
            self.db, 'cal-partial', device,
            '2026-06-10T12:00:00Z', '2027-01-01T00:00:00Z',
        )
        self.work_order(aircraft)
        self.feed(device, 1)
        result = self.app.call('effectiveness.run', {'study_id': 's1', **self.params(minimum_aircraft=1)})['result']
        event = result['events'][0]
        self.assertFalse(event['eligible'])
        self.assertIn('calibration_gap', {reason['code'] for reason in event['exclusions']})

    def test_filters_apply_to_order_type_component_metric_and_aircraft_type(self):
        self.fleet(count=3)
        self.assertEqual(
            self.app.call('effectiveness.run', {'study_id': 'types', **self.params(order_types=['inspection'])})['result']['events'],
            [],
        )
        filtered = self.app.call('effectiveness.run', {'study_id': 'single', **self.params(aircraft='plane2', minimum_aircraft=1)})['result']
        self.assertEqual({event['aircraft'] for event in filtered['events']}, {'plane2'})
        typed = self.app.call('effectiveness.run', {'study_id': 'typed', **self.params(aircraft_types=['A320'])})['result']
        self.assertEqual(typed['candidate_orders'], 3)
        with self.assertRaises(PipelineError) as caught:
            self.app.call('effectiveness.run', {'study_id': 'bad', **self.params(aircraft_types=['NOMATCH'])})
        self.assertEqual(caught.exception.code, 'unknown_aircraft_type')

    def test_cancelled_and_open_orders_are_not_candidates(self):
        aircraft, device = self.register_aircraft(1)
        self.work_order(aircraft, state='open')
        self.feed(device, 1)
        result = self.app.call('effectiveness.run', {'study_id': 's1', **self.params(minimum_aircraft=1)})['result']
        self.assertEqual(result['matching_work_orders'], 0)
        self.assertEqual(result['candidate_orders'], 0)
        self.assertEqual(result['events'], [])

    def test_completion_time_interval_filter(self):
        self.fleet(count=2)
        result = self.app.call('effectiveness.run', {
            'study_id': 'windowed', **self.params(minimum_aircraft=1),
            'start': '2026-06-10T13:00:00Z', 'end': '2026-07-01T00:00:00Z',
        })['result']
        self.assertEqual(result['matching_work_orders'], 0)
        bounded = self.app.call('effectiveness.run', {
            'study_id': 'bounded', **self.params(minimum_aircraft=1),
            'start': '2026-06-10T00:00:00Z', 'end': '2026-06-11T00:00:00Z',
        })['result']
        self.assertEqual(bounded['matching_work_orders'], 2)

    def test_orders_without_matching_devices_are_counted_separately(self):
        self.fleet(count=2)
        self.app.call('order.upsert', {'value': {
            'order_id': 'wo-orphan', 'revision': 1, 'aircraft': 'plane1', 'component': 'apu',
            'opened_at': (ANCHOR - dt.timedelta(hours=2)).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'closed_at': ANCHOR.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'issue': 'vibration', 'resolution': 'inspected', 'state': 'closed',
            'order_type': 'inspection',
        }})
        result = self.app.call('effectiveness.run', {'study_id': 's1', **self.params(minimum_aircraft=2)})['result']
        self.assertEqual(result['matching_work_orders'], 3)
        self.assertEqual(result['candidate_orders'], 2)

    def test_same_inputs_rebuild_byte_identical_payload(self):
        self.fleet(count=2)
        first = self.app.call('effectiveness.run', {'study_id': 's1', **self.params(minimum_aircraft=2)})['result']
        second = self.app.call('effectiveness.run', {'study_id': 'copy', **self.params(minimum_aircraft=2)})['result']
        self.assertEqual(first['digest'], second['digest'])

    def test_metrics_are_summarized_independently_per_aircraft(self):
        for index in (1, 2):
            aircraft = f'plane{index}'
            self.app.call('fleet.register', {'aircraft': aircraft, 'aircraft_type': 'A320'})
            for metric, unit, pre, post in (
                ('vibration', 'm/s2', '5.0', '2.0'),
                ('temperature', 'C', '60.0', '55.0'),
            ):
                device = f'{aircraft}-eng1-{metric}'
                devices.register(self.db, device, aircraft, 'eng1', metric, unit)
                devices.add_alias(
                    self.db, 'feed1', device + '-ext', device,
                    '2026-01-01T00:00:00Z', '2027-01-01T00:00:00Z',
                )
                devices.add_calibration(
                    self.db, f'cal-{device}', device,
                    '2026-01-01T00:00:00Z', '2027-01-01T00:00:00Z',
                )
                rules.configure(self.db, device, minimum='-1000', maximum='1000')
                rows = [{
                    'event_id': f'{metric}-{index}-{hour}',
                    'device': device + '-ext',
                    'observed_at': (ANCHOR - dt.timedelta(hours=24 - hour)).strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'value': pre if hour < 24 else post, 'unit': unit,
                } for hour in range(48)]
                batches.stage(self.db, f'{metric}-{index}', 'feed1', sensors.encode(rows))
                processor.drain(self.db, f'{metric}-{index}')
            self.work_order(aircraft)
        result = self.app.call('effectiveness.run', {
            'study_id': 'multi', 'minimum_aircraft': 2, 'bootstrap_replicates': 200,
            'window_seconds': 86400, 'resample_seconds': 3600, 'minimum_buckets': 6,
        })['result']
        summaries = {row['metric']: row for row in result['fleet_summary']}
        self.assertEqual(set(summaries), {'vibration', 'temperature'})
        self.assertEqual(summaries['vibration']['effect_median'], '-3')
        self.assertEqual(summaries['temperature']['effect_median'], '-5')
        self.assertEqual(summaries['temperature']['unit'], 'C')
        self.assertEqual(len(result['aircraft_effects']), 4)


class StudyStatisticsTests(StudyCase):
    def test_descriptive_only_below_aircraft_threshold(self):
        self.fleet(count=2)
        result = self.app.call('effectiveness.run', {'study_id': 's1', **self.params()})['result']
        summary = result['fleet_summary'][0]
        self.assertEqual(summary['inference'], 'descriptive_only')
        self.assertEqual(summary['reason'], 'below_minimum_aircraft')
        self.assertIsNone(summary['interval'])

    def test_cluster_bootstrap_resamples_whole_aircraft_deterministically(self):
        effects = {'a': '1', 'b': '2', 'c': '3', 'd': '4', 'e': '5'}
        first = statistics.cluster_bootstrap(effects, replicates=500, confidence='0.95', seed=42)
        second = statistics.cluster_bootstrap(effects, replicates=500, confidence='0.95', seed=42)
        self.assertEqual(first, second)
        self.assertEqual(first['aircraft_per_replicate'], 5)
        self.assertLessEqual(float(first['mean']['lower']), float(first['mean']['upper']))
        other = statistics.cluster_bootstrap(effects, replicates=500, confidence='0.95', seed=43)
        self.assertNotEqual(first['seed'], other['seed'])
        with self.assertRaises(PipelineError):
            statistics.cluster_bootstrap({'a': '1'}, replicates=500, confidence='0.95', seed=1)

    def test_fleet_interval_when_enough_aircraft_and_improvement_direction(self):
        self.fleet(count=5)
        result = self.app.call('effectiveness.run', {'study_id': 's1', **self.params()})['result']
        summary = result['fleet_summary'][0]
        self.assertEqual(summary['inference'], 'bootstrap_interval')
        self.assertEqual(summary['aircraft'], 5)
        self.assertEqual(summary['improved_aircraft'], 5)
        self.assertLess(float(summary['interval']['mean']['upper']), 0)
        self.assertEqual(summary['interval']['method'], 'aircraft_cluster_bootstrap_percentile')

    def test_undirected_study_reports_no_improvement_flag(self):
        self.fleet(count=2)
        result = self.app.call(
            'effectiveness.run',
            {'study_id': 's1', **self.params(minimum_aircraft=2, direction='undirected')},
        )['result']
        self.assertIsNone(result['fleet_summary'][0]['improved_aircraft'])
        self.assertTrue(all(event['improved'] is None for event in result['events']))


class StudyVersioningTests(StudyCase):
    def test_rerun_is_unchanged_but_corrections_create_revisions_and_diff(self):
        self.fleet(count=5)
        first = self.app.call('effectiveness.run', {'study_id': 's1', **self.params()})
        self.assertEqual(first['status'], 'created')
        again = self.app.call('effectiveness.run', {'study_id': 's1', **self.params()})
        self.assertEqual(again['status'], 'unchanged')
        self.assertEqual(again['revision'], 1)

        self.work_order('plane1', revision=2, opened_hours=4)
        second = self.app.call('effectiveness.run', {'study_id': 's1', **self.params()})
        self.assertEqual(second['status'], 'updated')
        self.assertEqual(second['revision'], 2)
        old = self.app.call('effectiveness.get', {'study_id': 's1', 'revision': 1})['result']
        self.assertEqual(old['frozen_inputs']['work_orders'][0]['revision'], 1)
        diff = self.app.call('effectiveness.compare', {'study_id': 's1', 'before': 1, 'after': 2})
        self.assertEqual(diff['changed_work_orders'][0]['after']['revision'], 2)
        changed = next(row for row in diff['changed_events'] if row['order_id'] == 'wo-plane1')
        self.assertNotEqual(changed['before_maintenance_duration_us'], changed['after_maintenance_duration_us'])
        self.assertEqual(len(self.app.call('effectiveness.revisions', {'study_id': 's1'})), 2)

    def test_calibration_correction_marks_changed_events(self):
        self.fleet(count=5)
        self.app.call('effectiveness.run', {'study_id': 's1', **self.params()})
        devices.correct_calibration(self.db, 'cal-plane2', 1, '1.1', '0', 0)
        recalibration.apply(self.db, 'cal-plane2', 2)
        second = self.app.call('effectiveness.run', {'study_id': 's1', **self.params()})
        self.assertEqual(second['revision'], 2)
        diff = self.app.call('effectiveness.compare', {'study_id': 's1', 'before': 1, 'after': 2})
        self.assertEqual(diff['calibration_revisions_added'], [{'calibration_id': 'cal-plane2', 'revision': 2}])
        changed = diff['changed_events']
        self.assertEqual({row['order_id'] for row in changed}, {'wo-plane2'})
        self.assertNotEqual(changed[0]['before_delta'], changed[0]['after_delta'])
        current = self.app.call('effectiveness.get', {'study_id': 's1'})
        self.assertEqual(current['current_revision'], 2)

    def test_stored_payload_is_digest_verified(self):
        self.fleet(count=2)
        self.app.call('effectiveness.run', {'study_id': 's1', **self.params(minimum_aircraft=2)})
        self.db.execute("UPDATE effectiveness_versions SET digest='tampered'")
        with self.assertRaises(PipelineError) as caught:
            effectiveness.get(self.db, 's1')
        self.assertEqual(caught.exception.code, 'study_corrupt')
        checked = self.app.call('inspect.check')
        self.assertFalse(checked['ok'])
        self.assertIn('study_digest', {error['code'] for error in checked['errors']})
        self.assertEqual(self.app.call('inspect.overview')['effectiveness_studies'], 1)

    def test_unknown_study_and_revision_are_reported(self):
        with self.assertRaises(PipelineError) as caught:
            self.app.call('effectiveness.get', {'study_id': 'missing'})
        self.assertEqual(caught.exception.code, 'unknown_study')
        self.fleet(count=2)
        self.app.call('effectiveness.run', {'study_id': 's1', **self.params(minimum_aircraft=2)})
        with self.assertRaises(PipelineError):
            self.app.call('effectiveness.get', {'study_id': 's1', 'revision': 9})

    def test_event_id_is_stable_across_runs(self):
        self.fleet(count=2)
        first = self.app.call('effectiveness.run', {'study_id': 's1', **self.params(minimum_aircraft=2)})['result']
        expected = digest({'order_id': 'wo-plane1', 'device_id': 'plane1-eng1-vib'})
        self.assertIn(expected, {event['event_id'] for event in first['events']})


class RequestValidationTests(StudyCase):
    def test_window_and_statistical_parameters_are_validated(self):
        cases = [
            {'window_seconds': 10},
            {'resample_seconds': 90000},
            {'minimum_coverage_ratio': '0'},
            {'minimum_coverage_ratio': '1.5'},
            {'confidence_level': '1'},
            {'bootstrap_replicates': 1},
            {'direction': 'sideways'},
        ]
        for override in cases:
            with self.assertRaises(PipelineError):
                self.app.call('effectiveness.run', {'study_id': 'bad', **self.params(**override)})

    def test_filter_lists_must_be_unique_and_bounded(self):
        with self.assertRaises(PipelineError):
            self.app.call('effectiveness.run', {'study_id': 'bad', **self.params(metrics=['vibration', 'vibration'])})
        with self.assertRaises(PipelineError):
            self.app.call('effectiveness.run', {'study_id': 'bad', **self.params(metrics=[])})


class BatchAtomicityTests(StudyCase):
    def test_failed_batch_rolls_back_fleet_registration_and_study(self):
        self.app.call('fleet.register', {'aircraft': 'p1', 'aircraft_type': 'A320'})
        before = self.app.call('fleet.list')
        with self.assertRaises(PipelineError):
            self.app.batch([
                {'operation': 'fleet.register', 'parameters': {'aircraft': 'p3', 'aircraft_type': 'A320'}},
                {'operation': 'effectiveness.run', 'parameters': {'study_id': 'bad', 'window_seconds': 1}},
            ])
        self.assertEqual(self.app.call('fleet.list'), before)
        self.assertEqual(self.app.call('effectiveness.list'), [])

    def test_study_runs_inside_successful_batch(self):
        aircraft, device = self.register_aircraft(1)
        self.work_order(aircraft)
        self.feed(device, 1)
        outcome = self.app.batch([{
            'operation': 'effectiveness.run',
            'parameters': {'study_id': 'batched', **self.params(minimum_aircraft=1, bootstrap_replicates=50)},
        }])
        self.assertEqual(outcome[0]['status'], 'created')
        self.assertEqual(self.app.call('effectiveness.get', {'study_id': 'batched'})['revision'], 1)


class SchemaMigrationTests(StudyCase):
    def test_database_created_before_studies_gains_order_type_and_tables(self):
        from hangar_ingest.runs.database import SCHEMA
        legacy = SCHEMA.replace(',order_type TEXT', '')
        for table in ('aircraft_registry', 'effectiveness_studies', 'effectiveness_versions'):
            legacy = re.sub(rf'CREATE TABLE IF NOT EXISTS {table}\(.*?\);', '', legacy, flags=re.S)
        target = Path(self.directory.name) / 'legacy.db'
        connection = sqlite3.connect(str(target))
        connection.executescript(legacy)
        connection.execute(
            "INSERT INTO work_orders(order_id,revision,aircraft,component,opened_at,closed_at,issue,resolution,"
            "state,payload_hash,updated_at) VALUES('o1',1,'p1','c1','2026-01-01T00:00:00Z','2026-01-01T01:00:00Z',"
            "'i','r','closed','h','t')",
        )
        connection.commit()
        connection.close()
        self.db.close()
        migrated = Database(target, clock=self.clock)
        app = Application(migrated)
        self.assertIsNone(app.call('order.get', {'order_id': 'o1'})['order_type'])
        self.assertEqual(app.call('effectiveness.list'), [])
        self.assertTrue(migrated.integrity()['ok'])
        migrated.close()


class StudyDemoTests(StudyCase):
    def test_demo_covers_exclusion_interval_and_revision_diff(self):
        from hangar_ingest.studies.demo import build
        report = build(self.app, self.db)
        self.assertEqual(report['first_run']['status'], 'created')
        self.assertEqual(report['second_run']['status'], 'updated')
        self.assertEqual(len(report['eligible_aircraft']), 4)
        self.assertEqual(
            {row['order_id'] for row in report['excluded']},
            {'wo-plane5', 'wo-plane5-overlap'},
        )
        summary = report['fleet_summary'][0]
        self.assertEqual(summary['inference'], 'bootstrap_interval')
        self.assertEqual(summary['aircraft'], 4)
        self.assertLess(float(summary['interval']['mean']['upper']), 0)
        changed = report['revision_diff']['changed_events']
        self.assertEqual([row['order_id'] for row in changed], ['wo-plane2'])
        self.assertEqual(
            report['revision_diff']['calibration_revisions_added'],
            [{'calibration_id': 'cal-plane2', 'revision': 2}],
        )

