from peewee import *
import datetime

db = SqliteDatabase('my_database.db')

class BaseModel(Model):
    class Meta:
        database = db

class Batch():
    created_date = DateTimeField(default=datetime.datetime.now)

class BatchStatus():
    batch = ForeignKeyField(Batch, backref='batches')
    status = CharField()
    created_date = DateTimeField(default=datetime.datetime.now)

class Document(BaseModel):
    filename = CharField()
    batch = ForeignKeyField(Batch, backref="batches")
    created_date = DateTimeField(default=datetime.datetime.now)

class DocumentStatus(BaseModel):
    document = ForeignKeyField(Document, backref='documents')
    status = CharField()
    created_date = DateTimeField(default=datetime.datetime.now)


