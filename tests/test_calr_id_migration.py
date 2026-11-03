"""Tests for the add_calr_submission_calr_id migration.

Each test downgrades the shared test database to the revision just before the
migration, builds the table state it cares about, runs the migration through
alembic, and checks the result. The fixture always upgrades back to head, so
the other test modules keep seeing the current schema.

MySQL normalises JSON on storage, so metadata is compared by reading the values
back from MySQL before and after the upgrade, never against the Python text it
was inserted from.
"""
import json
import re
import secrets
import uuid

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError

from dataregistry.api.db import DataRegistryReadWriteDB

PRE_CALR_ID_REVISION = 'create_sgc_gwas_munge_results'
UNIQUE_KEY = 'calr_submissions_calr_id_uniq'
CALR_ID_PATTERN = re.compile(r'^CalR[1-9A-HJ-NP-Za-km-z]{8}$')
ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'

engine = DataRegistryReadWriteDB().get_engine()

# alembic.ini has no path_separator setting, so every alembic command warns
# about it; that is a property of the ini file, not of what these tests check.
pytestmark = pytest.mark.filterwarnings("ignore:No path_separator found in configuration:DeprecationWarning")


def _truncate_submissions():
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE calr_submissions"))


def _drop_calr_id_leftovers():
    """Remove a calr_id column or key left behind by a run that died part-way,
    so every test starts from the real pre-migration schema."""
    with engine.begin() as conn:
        if _index_exists(conn):
            conn.execute(text(f"ALTER TABLE calr_submissions DROP INDEX `{UNIQUE_KEY}`"))
        if _column_info(conn) is not None:
            conn.execute(text("ALTER TABLE calr_submissions DROP COLUMN calr_id"))


@pytest.fixture
def pre_calr_id_schema():
    """Yield an alembic Config with calr_submissions empty and at the schema
    that has no calr_id, and always leave the database at head."""
    cfg = Config("./alembic.ini")
    # Setup is inside the try so a failure after the downgrade still reaches the
    # upgrade below; if the downgrade did nothing, upgrading to head is a no-op.
    try:
        command.downgrade(cfg, PRE_CALR_ID_REVISION)
        _drop_calr_id_leftovers()
        _truncate_submissions()
        yield cfg
    finally:
        command.upgrade(cfg, "head")
        _truncate_submissions()


def _column_info(conn):
    row = conn.execute(text("""
        SELECT COLLATION_NAME AS collation_name, IS_NULLABLE AS is_nullable
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'calr_submissions'
          AND column_name = 'calr_id'
    """)).mappings().first()
    return dict(row) if row else None


def _index_exists(conn):
    return conn.execute(text("""
        SELECT COUNT(*) FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'calr_submissions'
          AND index_name = :name
    """), {'name': UNIQUE_KEY}).scalar() > 0


def _insert(conn, name, metadata=None, calr_id=None):
    """Insert a submission. `metadata` is JSON text (or None for SQL NULL);
    `calr_id` is omitted from the INSERT when None (the column may not exist)."""
    columns = ['id', 'name', 'uploaded_by', 'metadata']
    params = {
        'id': uuid.uuid4().hex,
        'name': name,
        'uploaded_by': 'migration-test',
        'metadata': metadata,
    }
    if calr_id is not None:
        columns.append('calr_id')
        params['calr_id'] = calr_id
    conn.execute(text(
        f"INSERT INTO calr_submissions ({', '.join(columns)}) "
        f"VALUES ({', '.join(':' + c for c in columns)})"
    ), params)


def _read_metadata(conn):
    """{name: parsed metadata} as MySQL currently stores it (None for NULL)."""
    result = {}
    for row in conn.execute(text("SELECT name, metadata FROM calr_submissions")).mappings():
        raw = row['metadata']
        result[row['name']] = json.loads(raw) if isinstance(raw, str) else raw
    return result


def _read_calr_ids(conn):
    return {
        row['name']: row['calr_id']
        for row in conn.execute(text("SELECT name, calr_id FROM calr_submissions")).mappings()
    }


def _has_experiment_id(metadata):
    return isinstance(metadata, dict) and 'experiment_id' in metadata


def _expected_after_strip(metadata):
    """What the migration should leave in the metadata column for this value."""
    if not _has_experiment_id(metadata):
        return metadata
    remaining = {k: v for k, v in metadata.items() if k != 'experiment_id'}
    return remaining or None


def _assert_calr_id_column_is_final(conn):
    info = _column_info(conn)
    assert info is not None
    assert info['collation_name'] == 'utf8mb4_bin'
    assert info['is_nullable'] == 'NO'
    assert _index_exists(conn)


def _add_stale_nullable_column(conn):
    """The state an earlier revision of this migration left behind: a nullable
    calr_id in the table's default (case-insensitive) collation plus its key."""
    conn.execute(text(
        "ALTER TABLE calr_submissions "
        "ADD COLUMN calr_id varchar(16) NULL, "
        f"ADD UNIQUE KEY `{UNIQUE_KEY}` (calr_id)"
    ))
    info = _column_info(conn)
    assert info['collation_name'] != 'utf8mb4_bin'
    assert info['is_nullable'] == 'YES'


def _scripted_choice(ids):
    """A stand-in for secrets.choice that spells out `ids` (without their CalR
    prefix) one character per call, then falls back to the real thing."""
    chars = iter(''.join(i[len('CalR'):] for i in ids))
    real_choice = secrets.choice

    def choice(seq):
        try:
            return next(chars)
        except StopIteration:
            return real_choice(seq)

    return choice


def _backfill_metadata_cases():
    """Metadata JSON text for 60 rows, in six shapes."""
    cases = []
    for i in range(60):
        kind = i % 6
        if kind == 0:
            cases.append(json.dumps({'experiment_id': f'EXP-{i}', 'strain': 'C57BL/6J', 'n_animals': i}))
        elif kind == 1:
            cases.append(json.dumps({'experiment_id': f'EXP-{i}'}))
        elif kind == 2:
            cases.append(json.dumps({
                'strain': 'ob/ob',
                'notes': {'experiment_id': 'nested-stays'},
                'tags': ['a', 'b'],
                'weight': 21.5,
            }))
        elif kind == 3:
            cases.append(None)
        elif kind == 4:
            cases.append(json.dumps({}))
        else:
            cases.append(json.dumps(['experiment_id']))
    return cases


def test_backfill_assigns_distinct_ids_and_strips_experiment_id(pre_calr_id_schema):
    with engine.begin() as conn:
        for i, metadata in enumerate(_backfill_metadata_cases()):
            _insert(conn, f'sub-{i}', metadata)
        before = _read_metadata(conn)

    assert len(before) == 60
    assert sum(1 for m in before.values() if _has_experiment_id(m)) == 20

    command.upgrade(pre_calr_id_schema, "head")

    with engine.begin() as conn:
        calr_ids = _read_calr_ids(conn)
        after = _read_metadata(conn)
        _assert_calr_id_column_is_final(conn)

    assert len(calr_ids) == 60
    assert all(CALR_ID_PATTERN.match(v) for v in calr_ids.values())
    assert len(set(calr_ids.values())) == 60

    assert after.keys() == before.keys()
    for name, original in before.items():
        assert after[name] == _expected_after_strip(original), name

    # spot checks on the shapes the loop above covers
    assert after['sub-0'] == {'strain': 'C57BL/6J', 'n_animals': 0}
    assert after['sub-1'] is None
    assert after['sub-2']['notes'] == {'experiment_id': 'nested-stays'}
    assert after['sub-3'] is None
    assert after['sub-4'] == {}
    assert after['sub-5'] == ['experiment_id']


def test_resume_after_partial_run_normalises_column_and_keeps_assigned_ids(pre_calr_id_schema):
    with engine.begin() as conn:
        _add_stale_nullable_column(conn)

        # 15 rows the earlier revision had already reached
        for i in range(15):
            if i == 0:
                # the earlier run died between a row's calr_id and metadata
                # updates; rows that already have a calr_id are not revisited
                metadata = json.dumps({'experiment_id': 'left-alone', 'strain': 'x'})
            else:
                metadata = json.dumps({'strain': 'x'})
            _insert(conn, f'done-{i}', metadata, calr_id='CalR' + ALPHABET[i] * 8)
        # 25 rows it had not
        for i in range(25):
            metadata = json.dumps({'experiment_id': f'EXP-{i}', 'strain': 'y'}) if i % 2 == 0 else None
            _insert(conn, f'todo-{i}', metadata)

        before_ids = _read_calr_ids(conn)
        before_metadata = _read_metadata(conn)

    command.upgrade(pre_calr_id_schema, "head")

    with engine.begin() as conn:
        after_ids = _read_calr_ids(conn)
        after_metadata = _read_metadata(conn)
        _assert_calr_id_column_is_final(conn)

    assert len(after_ids) == 40
    assert None not in after_ids.values()
    assert len(set(after_ids.values())) == 40

    for name, calr_id in before_ids.items():
        if calr_id is not None:
            assert after_ids[name] == calr_id
            assert after_metadata[name] == before_metadata[name]
        else:
            assert CALR_ID_PATTERN.match(after_ids[name])
            assert after_metadata[name] == _expected_after_strip(before_metadata[name])
    assert after_metadata['done-0'] == {'experiment_id': 'left-alone', 'strain': 'x'}


def test_backfill_redraws_on_collision_and_compares_case_sensitively(pre_calr_id_schema, monkeypatch):
    with engine.begin() as conn:
        _add_stale_nullable_column(conn)
        _insert(conn, 'assigned-digits', calr_id='CalR11111111')
        _insert(conn, 'assigned-upper', calr_id='CalRAAAAAAAA')
        _insert(conn, 'unassigned')

    # First draw repeats an assigned id exactly, so it has to be redrawn. The
    # second differs from another assigned id only by case, which is a
    # different id under utf8mb4_bin, so it has to be accepted as drawn.
    with monkeypatch.context() as m:
        m.setattr(secrets, 'choice', _scripted_choice(['CalR11111111', 'CalRaaaaaaaa']))
        command.upgrade(pre_calr_id_schema, "head")

    with engine.begin() as conn:
        calr_ids = _read_calr_ids(conn)

    assert calr_ids == {
        'assigned-digits': 'CalR11111111',
        'assigned-upper': 'CalRAAAAAAAA',
        'unassigned': 'CalRaaaaaaaa',
    }


def test_failed_backfill_can_be_rerun(pre_calr_id_schema, monkeypatch):
    with engine.begin() as conn:
        for i in range(3):
            _insert(conn, f'sub-{i}', json.dumps({'experiment_id': f'EXP-{i}', 'strain': 'x'}))
        before = _read_metadata(conn)

    # Every draw is CalR11111111: the first row gets it, the second exhausts its
    # five draws and the migration aborts after the column and key were added.
    with monkeypatch.context() as m:
        m.setattr(secrets, 'choice', lambda seq: seq[0])
        with pytest.raises(RuntimeError):
            command.upgrade(pre_calr_id_schema, "head")

    with engine.begin() as conn:
        info = _column_info(conn)
        assert info is not None and info['is_nullable'] == 'YES'

    command.upgrade(pre_calr_id_schema, "head")

    with engine.begin() as conn:
        calr_ids = _read_calr_ids(conn)
        after = _read_metadata(conn)
        _assert_calr_id_column_is_final(conn)

    assert len(set(calr_ids.values())) == 3
    assert all(CALR_ID_PATTERN.match(v) for v in calr_ids.values())
    for name, original in before.items():
        assert after[name] == _expected_after_strip(original)


def test_calr_id_at_head_is_case_sensitive_unique_and_required(pre_calr_id_schema):
    command.upgrade(pre_calr_id_schema, "head")

    with engine.begin() as conn:
        _insert(conn, 'lower', calr_id='CalRabcdefgh')
        _insert(conn, 'upper', calr_id='CalRABCDEFGH')

    with engine.begin() as conn:
        def count(calr_id):
            return conn.execute(
                text("SELECT COUNT(*) FROM calr_submissions WHERE calr_id = :calr_id"),
                {'calr_id': calr_id},
            ).scalar()

        assert count('CalRabcdefgh') == 1
        assert count('CalRABCDEFGH') == 1
        assert count('CalRAbcdefgh') == 0

    with pytest.raises(IntegrityError):
        with engine.begin() as conn:
            _insert(conn, 'duplicate', calr_id='CalRabcdefgh')

    with pytest.raises(IntegrityError):
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO calr_submissions (id, name, uploaded_by, calr_id) "
                "VALUES (:id, 'null-id', 'migration-test', NULL)"
            ), {'id': uuid.uuid4().hex})

    # the column has no default, so code that predates calr_id cannot insert
    with pytest.raises(DBAPIError):
        with engine.begin() as conn:
            _insert(conn, 'no-calr-id')
