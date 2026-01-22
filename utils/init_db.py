from utils.db import Base, engine


def create_all_tables():
    Base.metadata.create_all(bind=engine)
