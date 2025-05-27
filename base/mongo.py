from pymongo.database import Database

def clear(db: Database):
    for collection in db.list_collection_names():
        db.drop_collection(collection)