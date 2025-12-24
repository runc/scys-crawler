import datetime

from peewee import (
    AutoField,
    BooleanField,
    DateTimeField,
    IntegerField,
    Model,
    TextField,
)

from .base import db


class SCProfile(Model):
    id = AutoField()
    created_at = DateTimeField(default=datetime.datetime.now)
    updated_at = DateTimeField(default=datetime.datetime.now)
    user_id = IntegerField(unique=True, index=True)
    name = TextField(null=True)
    avatar = TextField(null=True)
    xq_user_id = IntegerField(null=True)
    xq_group_number = IntegerField(null=True)
    introduction = TextField(null=True)
    province = TextField(null=True)
    city = TextField(null=True)
    district = TextField(null=True)
    gender = TextField(null=True)
    follow_count = IntegerField(default=0)
    follower_count = IntegerField(default=0)
    mutual_follow_count = IntegerField(default=0)
    total_like_and_coin_count = IntegerField(default=0)
    is_navigator = BooleanField(default=False)
    navigator_expire_time = TextField(null=True)
    date_expire = TextField(null=True)
    privacy_settings = TextField(null=True)
    follow_status = IntegerField(default=0)
    profile_json = TextField(null=True)

    class Meta:
        database = db
        table_name = "sc_profile"


SC_PROFILE_COLUMN_DEFINITIONS = {
    "user_id": "INTEGER UNIQUE",
    "name": "TEXT",
    "avatar": "TEXT",
    "xq_user_id": "INTEGER",
    "xq_group_number": "INTEGER",
    "introduction": "TEXT",
    "province": "TEXT",
    "city": "TEXT",
    "district": "TEXT",
    "gender": "TEXT",
    "follow_count": "INTEGER DEFAULT 0",
    "follower_count": "INTEGER DEFAULT 0",
    "mutual_follow_count": "INTEGER DEFAULT 0",
    "total_like_and_coin_count": "INTEGER DEFAULT 0",
    "is_navigator": "INTEGER DEFAULT 0",
    "navigator_expire_time": "TEXT",
    "date_expire": "TEXT",
    "privacy_settings": "TEXT",
    "follow_status": "INTEGER DEFAULT 0",
    "profile_json": "TEXT",
}


def ensure_sc_profile_schema() -> None:
    table_name = SCProfile._meta.table_name

    if not db.table_exists(table_name):
        db.create_tables([SCProfile])
        return

    existing_columns = {column.name for column in db.get_columns(table_name)}
    missing_columns = [
        (column_name, column_sql)
        for column_name, column_sql in SC_PROFILE_COLUMN_DEFINITIONS.items()
        if column_name not in existing_columns
    ]

    for column_name, column_sql in missing_columns:
        db.execute_sql(
            f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}"
        )
