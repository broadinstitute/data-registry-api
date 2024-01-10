from uuid import UUID

from bioindex.lib.index import Index
from bioindex.lib import config


def create_new_bioindex(engine, idx_uuid: UUID, s3_path, schema):
    try:
        idx_name = str(idx_uuid)
        existing_index = Index.lookup_all(engine, idx_name)[0]
    except KeyError:
        existing_index = None
    if not existing_index:
        Index.create(engine, idx_name, idx_name, s3_path, schema)
    try:
        new_index = Index.lookup(engine, idx_name, schema.count(',') + 1)
        new_index.prepare(engine, rebuild=True)
        new_index.build(config.Config(), engine)
    except Exception as e:
        print(f"Failed to create index {idx_name} with error {e}")
        raise e
