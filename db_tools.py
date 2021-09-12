from abc import ABCMeta, abstractmethod

from pymongo import MongoClient

MONGO_HOST = 'localhost:27017'
FILES_COLLECTION = "files_data"
DIRECTORIES_COLLECTION = "directories_data"
ERRORS_COLLECTION = "indexers_errors"


class DBWrapper(metaclass=ABCMeta):
    @abstractmethod
    def save_file_data(self, db_name, file_data):
        pass

    @abstractmethod
    def save_directory_data(self, db_name, directory_data):
        pass

    @abstractmethod
    def save_error_data(self, db_name, error_data):
        pass


class MongoWrapper(DBWrapper):
    def __init__(self):
        self.client = MongoClient(host=MONGO_HOST)

    def _save_data(self, db_name, collection, data_obj):
        self.client[db_name][collection].insert_one(data_obj.__dict__)

    def save_file_data(self, db_name, file_data):
        self._save_data(db_name, FILES_COLLECTION, file_data)

    def save_directory_data(self, db_name, directory_data):
        self._save_data(db_name, DIRECTORIES_COLLECTION, directory_data)

    def save_error_data(self, db_name, error_data):
        self._save_data(db_name, ERRORS_COLLECTION, error_data)

    def __del__(self):
        self.client.close()
