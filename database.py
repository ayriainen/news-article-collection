import datetime
from contextlib import contextmanager
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, Boolean, ForeignKey

#engine = create_engine('sqlite:///data.db', echo = True)
# connection pooling for concurrent access
engine = create_engine('sqlite:///data/data.db', echo = False, pool_size = 20, max_overflow = 0)
Session = sessionmaker(bind=engine)
meta = MetaData()

urls = Table('urls', meta,
    Column("id", Integer, primary_key = True ),
    Column("feed", String),
    Column("url", String),
    Column('storage_time', DateTime, default=datetime.datetime.now(datetime.UTC) ),
    Column("download_attempted", Boolean, default = False ),
    Column("article_id", ForeignKey("articles.id"), default = None )
)

articles = Table( 'articles', meta,
    Column('id', Integer, primary_key = True),
    Column('url', String),
    Column('html', String),
    Column('full_text', String),
    Column('time', DateTime),
    Column('download_time', DateTime, default=datetime.datetime.now(datetime.UTC) )
)

meta.create_all(engine)

# sessions instead of direct connections
# contextmanager for resource management of collect and process
@contextmanager
def session_scope():
    session = Session()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise
    finally:
        session.close()