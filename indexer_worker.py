from os import DirEntry
from abc import ABCMeta, abstractmethod
import data_objects
import queue_managment
import db_tools
import os
import json
import traceback


class IndexerWorker(metaclass=ABCMeta):
    def __init__(self, db_reporter, folder_scanner):
        self.db_reporter = db_reporter
        self.folder_scanner = folder_scanner

    @abstractmethod
    def run_task(self, task):
        pass


class QueueIndexerWorker(IndexerWorker):
    def __init__(self, db_reporter: db_tools.DBWrapper, folder_scanner,
                 queue_wrapper: queue_managment.TasksQueueWrapper):
        super().__init__(db_reporter, folder_scanner)
        self.queue_wrapper = queue_wrapper

    def run_task(self, task):
        db_name = task.task_db
        directory_path = task.directory_path
        try:
            directory_item: DirEntry
            for directory_item in self.folder_scanner(directory_path):
                if directory_item.is_dir() and not directory_item.is_symlink():
                    self.db_reporter.save_directory_data(db_name,
                                                         data_objects.parse_directory_dir_entry(directory_item))
                    if task.recursive:
                        new_task = data_objects.TaskData(directory_path=directory_item.path, task_db=task.task_db,
                                                         recursive=True)
                        self.queue_wrapper.publish_task(task_body=new_task.to_json())
                elif directory_item.is_file():
                    self.db_reporter.save_file_data(db_name, data_objects.parse_file_dir_entry(directory_item))
        except:
            error_data = data_objects.ErrorData(path=directory_path, stack_trace=traceback.format_exc())
            self.db_reporter.save_error_data(db_name, error_data)


def start_worker(queue_wrapper: queue_managment.TasksQueueWrapper, worker: IndexerWorker):
    def _call_run_task(body):
        task = data_objects.parse_task_data(json.loads(body))
        worker.run_task(task)

    queue_wrapper.register_consumer_callback(_call_run_task)
    queue_wrapper.start_consumer()


def main():
    db_reporter = db_tools.MongoWrapper()
    scanner = os.scandir
    queue_wrapper = queue_managment.RabbitMQWrapper()
    worker = QueueIndexerWorker(db_reporter, scanner, queue_wrapper)
    start_worker(queue_wrapper, worker)


if __name__ == '__main__':
    main()
