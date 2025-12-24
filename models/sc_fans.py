from peewee import AutoField, CharField, IntegerField, Model, TextField

from .base import db


class SCFans(Model):
    id = AutoField()
    union_user_id = IntegerField(unique=True, index=True)
    xq_group_number = IntegerField(null=True)
    user_name = CharField(max_length=255, null=True)
    avatar = TextField(null=True)
    introduction = TextField(null=True)
    follow_status = IntegerField(default=0)

    class Meta:
        database = db
        table_name = "sc_fans"
