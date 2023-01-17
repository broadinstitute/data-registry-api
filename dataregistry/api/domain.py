

class Record:
    def __init__(self, id, s3_bucket_id, name, description, created_at, deleted_at_unix_time):
        self.id = id
        self.s3_bucket_id = s3_bucket_id
        self.name = name
        self.description = description
        self.created_at = created_at
        self.deleted_at_unix_time = deleted_at_unix_time

    def to_json(self):
        return {
            "id": self.id,
            "s3_bucket_id": self.s3_bucket_id,
            "name": self.name,
            "description": self.description,
            "created_at": self.created_at,
            "deleted_at_unix_time": self.deleted_at_unix_time
        }
