import datetime

from typing import Iterable

from peewee import AutoField, DateTimeField, IntegerField, Model, TextField

from .base import db


class SCTopic(Model):
    id = AutoField()
    created_at = DateTimeField(default=datetime.datetime.now)
    updated_at = DateTimeField(default=datetime.datetime.now)
    topic_id = TextField(unique=True, index=True)
    user_id = IntegerField(null=True, index=True)
    topic_json = TextField(null=True)
    topic_created_dt = DateTimeField(null=True)
    topic_created_at = IntegerField(null=True)

    class Meta:
        database = db
        table_name = "sc_topic"


SCTOPIC_COLUMN_DEFINITIONS = {
    "topic_id": "TEXT",
    "user_id": "INTEGER",
    "topic_json": "TEXT",
}

UNIQUE_TOPIC_INDEX = "sctopic_topic_id_idx"
USER_INDEX = "sctopic_user_id_idx"
LEGACY_USER_UNIQUE_INDEX = "sctopic_user_id"


def ensure_sc_topic_schema() -> None:
    table_name = SCTopic._meta.table_name

    if not db.table_exists(table_name):
        db.create_tables([SCTopic])
        return

    existing_columns = {column.name for column in db.get_columns(table_name)}
    for column_name, column_def in SCTOPIC_COLUMN_DEFINITIONS.items():
        if column_name not in existing_columns:
            db.execute_sql(
                f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" {column_def}'
            )

    _ensure_user_id_nullable(table_name)
    _ensure_indexes(table_name)


def _ensure_user_id_nullable(table_name: str) -> None:
    user_columns: Iterable = [
        column for column in db.get_columns(table_name) if column.name == "user_id"
    ]
    if not user_columns:
        return

    user_column = next(iter(user_columns))
    if getattr(user_column, "null", True):
        return

    _relax_user_id_not_null(table_name)


def _relax_user_id_not_null(table_name: str) -> None:
    temp_table = f"{table_name}_tmp_migration"
    db.execute_sql(f'DROP TABLE IF EXISTS "{temp_table}"')
    db.execute_sql(
        f'''
        CREATE TABLE "{temp_table}" (
            "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            "created_at" DATETIME NOT NULL,
            "updated_at" DATETIME NOT NULL,
            "topic_id" TEXT,
            "user_id" INTEGER,
            "topic_json" TEXT,
            "topic_created_dt" DATETIME,
            "topic_created_at" INTEGER
        )
        '''
    )
    db.execute_sql(
        f'''
        INSERT INTO "{temp_table}" (
            "id",
            "created_at",
            "updated_at",
            "topic_id",
            "user_id",
            "topic_json",
            "topic_created_dt",
            "topic_created_at"
        )
        SELECT
            "id",
            "created_at",
            "updated_at",
            "topic_id",
            "user_id",
            "topic_json",
            "topic_created_dt",
            "topic_created_at"
        FROM "{table_name}"
        '''
    )
    db.execute_sql(f'DROP TABLE "{table_name}"')
    db.execute_sql(f'ALTER TABLE "{temp_table}" RENAME TO "{table_name}"')


def _ensure_indexes(table_name: str) -> None:
    db.execute_sql(f'DROP INDEX IF EXISTS "{LEGACY_USER_UNIQUE_INDEX}"')

    indexes = db.get_indexes(table_name)
    has_topic_unique = any(
        index.columns == ["topic_id"] and index.unique for index in indexes
    )
    if not has_topic_unique:
        db.execute_sql(
            f'CREATE UNIQUE INDEX IF NOT EXISTS "{UNIQUE_TOPIC_INDEX}" '
            f'ON "{table_name}" ("topic_id")'
        )

    indexes = db.get_indexes(table_name)
    has_user_index = any(index.columns == ["user_id"] for index in indexes)
    if not has_user_index:
        db.execute_sql(
            f'CREATE INDEX IF NOT EXISTS "{USER_INDEX}" '
            f'ON "{table_name}" ("user_id")'
        )
