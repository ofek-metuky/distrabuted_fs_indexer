import queue_managment
import data_objects
import indexer_worker
import time
import multiprocessing

WORKERS_AMOUNT = 100
RELAXING_TIME = 20


def _start_workers():
    workers = list()
    for _ in range(WORKERS_AMOUNT):
        worker = multiprocessing.Process(target=indexer_worker.main)
        worker.start()
        workers.append(worker)
    return workers


def main():
    workers = _start_workers()
    queue_client = queue_managment.RabbitMQWrapper()
    task = data_objects.TaskData(directory_path=r"c:/", task_db=f"indexer_db_{int(time.time())}", recursive=True)
    queue_client.publish_task(task.to_json())
    q_size = queue_client.get_queue_size()
    try:
        while True:
            print(f"size is: {q_size}")
            time.sleep(5)
            q_size = queue_client.get_queue_size()
    except KeyboardInterrupt:
        pass

    for worker in workers:
        worker.terminate()


if __name__ == '__main__':
    main()
