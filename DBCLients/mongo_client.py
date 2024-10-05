from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, PyMongoError

class MongoDBClient:
    url='mongodb+srv://user:leadSc0re@cluster0.2cfpn.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0'
    def __init__(self):
        self.client = MongoClient(MongoDBClient.url)
        self.database = self.client['LeadDB']

    def create_collection(self, collection_name):
        try:
            self.database.create_collection(collection_name)
            print(f"Collection '{collection_name}' created.")
        except PyMongoError as e:
            print(f"Error creating collection: {e}")

    def insert_document(self, collection_name, document):
        try:
            collection = self.database[collection_name]
            result = collection.insert_one(document)
            return result.inserted_id
        except DuplicateKeyError:
            print("Document already exists.")
        except PyMongoError as e:
            print(f"Error inserting document: {e}")

    def fetch_documents(self, collection_name, query=None):
        """Fetch documents from a collection."""
        try:
            collection = self.database[collection_name]
            documents = list(collection.find(query))
            return documents
        except PyMongoError as e:
            print(f"Error fetching documents: {e}")
            return []

    def update_document(self, collection_name, query, update_values):
        """Update a document in a collection."""
        try:
            collection = self.database[collection_name]
            result = collection.update_one(query, {'$set': update_values})
            return result.modified_count
        except PyMongoError as e:
            print(f"Error updating document: {e}")
            return 0

    def delete_document(self, collection_name, query):
        try:
            collection = self.database[collection_name]
            result = collection.delete_one(query)
            return result.deleted_count
        except PyMongoError as e:
            print(f"Error deleting document: {e}")
            return 0

    # Method for upserting a document into a collection
    def upsert(self, collection_name, filter_dict, update_dict):
        """
        Upsert a document into the specified collection.
        - collection_name: The name of the collection where the data should be upserted.
        - filter_dict: The filter criteria (typically _id or unique fields).
        - update_dict: The data to be set/updated.
        """
        collection = self.database[collection_name]
        collection.update_one(
            filter_dict,
            {'$set': update_dict},
            upsert=True
        )

    def close(self):
        self.client.close()
