import json
import os


class TaskData:
    def __init__(self, directory_path, task_db, recursive=True):
        self.directory_path = directory_path
        self.task_db = task_db
        self.recursive = recursive

    def to_json(self):
        return json.dumps(self.__dict__)


def parse_task_data(raw_data: dict):
    return TaskData(raw_data["directory_path"], raw_data["task_db"], raw_data.get("recursive", True))


def parse_file_dir_entry(entry: os.DirEntry):
    stat = entry.stat()
    return FileData(entry.name, entry.path, stat.st_ctime, stat.st_size)


def parse_directory_dir_entry(entry: os.DirEntry):
    stat = entry.stat()
    return DirectoryData(entry.name, entry.path, stat.st_ctime)


class DirectoryData:
    def __init__(self, name, path, creation_time):
        self.name = name
        self.path = path
        self.creation_time = creation_time


class FileData:
    def __init__(self, name, path, creation_time, size_in_bytes):
        self.name = name
        self.path = path
        self.creation_time = creation_time
        self.size_in_bytes = size_in_bytes


class ErrorData:
    def __init__(self, path, stack_trace):
        self.path = path
        self.stack_trace = stack_trace
