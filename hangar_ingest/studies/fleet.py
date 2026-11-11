from hangar_ingest.types import fail, identity, integer, text
from hangar_ingest.runs import journal


def register(db, aircraft, aircraft_type, revision=0):
    identity(aircraft, 'aircraft')
    aircraft_type = text(aircraft_type, 'aircraft_type', 120)
    integer(revision, 'revision')
    with db.transaction():
        previous = db.one('SELECT * FROM aircraft_registry WHERE aircraft=?', (aircraft,))
        if previous is None:
            if revision != 0:
                fail('stale_revision', 'Aircraft is not registered yet')
            db.execute(
                'INSERT INTO aircraft_registry(aircraft,aircraft_type,revision,created_at,updated_at) VALUES(?,?,1,?,?)',
                (aircraft, aircraft_type, db.clock(), db.clock()),
            )
            journal.record(db, 'aircraft.registered', {'aircraft': aircraft, 'aircraft_type': aircraft_type})
        else:
            if revision != previous['revision']:
                fail('stale_revision', 'Aircraft registry revision changed')
            if previous['aircraft_type'] == aircraft_type:
                return previous
            db.execute(
                'UPDATE aircraft_registry SET aircraft_type=?,revision=revision+1,updated_at=? WHERE aircraft=?',
                (aircraft_type, db.clock(), aircraft),
            )
            journal.record(db, 'aircraft.retyped', {'aircraft': aircraft, 'aircraft_type': aircraft_type, 'revision': previous['revision'] + 1})
        return db.one('SELECT * FROM aircraft_registry WHERE aircraft=?', (aircraft,))


def get(db, aircraft):
    identity(aircraft, 'aircraft')
    row = db.one('SELECT * FROM aircraft_registry WHERE aircraft=?', (aircraft,))
    if row is None:
        fail('unknown_aircraft', 'Aircraft is not registered')
    return row


def listing(db, aircraft_type=None):
    if aircraft_type is None:
        return db.all('SELECT * FROM aircraft_registry ORDER BY aircraft')
    identity(aircraft_type, 'aircraft_type')
    return db.all('SELECT * FROM aircraft_registry WHERE aircraft_type=? ORDER BY aircraft', (aircraft_type,))


def type_of(db, aircraft):
    row = db.one('SELECT aircraft_type FROM aircraft_registry WHERE aircraft=?', (aircraft,))
    return row['aircraft_type'] if row else None
