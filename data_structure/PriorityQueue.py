"""
A custom implementation of a thread-safe priority queue using the heapq module.

Then we have the StepQueue class, which is a wrapper around the PriorityQueue class.
It is used to manage the priority queues for each step in the pipeline.
It also has the ability to snapshot the current state of the queue and restore it later.
This is useful for when the program is interrupted and we want to continue from where we left off.
"""

import heapq
import os
import pickle
import platform
import threading
from typing import Union
from typing import Optional
from data_structure.Jobs import Job


class PriorityQueue:
    """
    PriorityQueue class is a custom implementation of a thread-safe priority queue using the heapq module.

    Could be initialized with a list of tuples, where the first element is the priority and the second is the job.

    Parameters
    ----------
    queue : `Optional[list[tuple[int, Job]]]`
        A list of tuples, where the first element is the priority and the second is the job.
    """

    def __init__(self, queue: Optional[list[tuple[int, Job]]] = None):
        if queue is None:
            queue = list()
        self._queue = queue

    def add_job(self, job: Job, priority: int) -> None:
        """
        Add a job to the queue with a given priority.
        
        Parameters
        ----------
        job : `Job`
            The job object to be added to the queue.
        priority : `int`
            The priority of the job, where 0 is the highest priority.
        """
        heapq.heappush(self._queue, (priority, job))

    def get_next_job(self) -> Optional[Job]:
        """
        Get the next job in the queue, taking into account the priority.
        
        Returns
        -------
        `Optional[Job]`
            The next job in the queue, or None if the queue is empty.
        """
        if self._queue:
            _, job = heapq.heappop(self._queue)
            return job
        else:
            return None

    def get_length(self):
        """
        Get the length of the queue.

        Returns
        -------
        `int`
            The length of the queue.
        """
        return len(self._queue)


class StepQueue:
    """
    StepQueue class is a wrapper around the PriorityQueue class.

    It is used to manage the priority queues for each step in the pipeline.
    It also has the ability to snapshot the current state of the queue and restore it later.
    This is useful for when the program is interrupted and we want to continue from where we left off.

    Parameters
    ----------
    steps : `Union[int, list[PriorityQueue]]`
        The number of steps in the pipeline, or a list of PriorityQueue objects.
    """
    def __init__(self, steps: Union[int, list[PriorityQueue]]):
        if isinstance(steps, int):
            self._queues = [PriorityQueue() for _ in range(steps)]
        elif isinstance(steps, list):
            self._queues = [PriorityQueue(queue._queue) for queue in steps]

        self.__transient__ = {'_lock'}
        self._lock = threading.Lock()
        self.pause = False
        self.pause_done = False

        self.restore()

    def add_job(self, step: int, job: Job, priority=5) -> None:
        """
        Add a job to the queue with a given priority. The job will be added to the queue of the given step.
        
        Parameters
        ----------
        step : `int`
            The step in the pipeline where the job will be added.
        job : `Job`
            The job object to be added to the queue.
        priority : `int, optional`
            The priority of the job, where 0 is the highest priority. By default `5`.
        """
        with self._lock:
            self._queues[step].add_job(job, priority)
            self.pause = False
            self.pause_done = False

    def get_next_job(self, step: Optional[int] = None) -> Optional[tuple[int, Optional[Job]]]:
        """
        Get the next job in the queue, taking into account the priority.
        If step is not provided, the queue with the most jobs will be selected.

        This method does not work with the done queues, which are the last two queues in the list.
        If done queues are wanted, use the get_done_job method instead.
        
        Parameters
        ----------
        step : `Optional[int], optional`
            The step in the pipeline where the job will be added. If not provided,
            the queue with the most jobs will be selected. By default `None`.
        
        Returns
        -------
        `Optional[tuple[int, Optional[Job]]]`
            A tuple containing the step and the next job in the queue, or None if the queue is empty.
        """
        with self._lock:
            if step is None:
                larger = 0
                for i, val in enumerate([queue.get_length() for queue in self._queues[:-2]]):
                    if val > larger:
                        larger = val
                        step = i

            if step is None:
                self.pause = True
                return None

            queue = self._queues[step]
            if queue:
                job = queue.get_next_job()
                if job is not None:
                    return step, job

            self.pause = True
            return None

    def get_done_job(self, step:int) -> Optional[tuple[int, Optional[Job]]]:
        """
        Get the next job in the done queue, taking into account the priority.

        This method only works with the done queues, which are the last two queues in the list.
        If normal queues are wanted, use the get_next_job method instead.
        
        Parameters
        ----------
        step : `int`
            The step in the pipeline where the job will be added.
            As done jobs should be managed by different threads, the step is required.
        
        Returns
        -------
        `Optional[tuple[int, Optional[Job]]]`
            A tuple containing the step and the next job in the queue, or None if the queue is empty.
        """
        with self._lock:

            queue = self._queues[step]
            if queue:
                job = queue.get_next_job()
                if job is not None:
                    return step, job

            self.pause_done = True
            return None

    def generate_temp_path(self):
        """
        Generate a temporary path to store the snapshot of the queue and return it.
        Manages various OS paths.

        If already exists, it will return the path.
        
        Returns
        -------
        `str`
            The temporary path to store the snapshot of the queue.
        
        Raises
        ------
        `Exception`
            If the OS is not supported.
        """
        if (platform.system() == "Windows"):
            path = os.path.join(os.environ["TEMP"], "DawBotcodingGym")

        elif (platform.system() == "Linux"):
            path = os.path.join("/tmp", "DawBotcodingGym")
        else:
            raise NotImplementedError("Unsupported OS")

        if not os.path.exists(path):
            os.makedirs(path)
        return path

    def snapshot(self):
        """
        Snapshot the current state of the queue and save it to a file.
        """
        with self._lock:
            path = self.generate_temp_path()

            with open(os.path.join(path, "queue_snapshot"), "wb") as file:
                pickle.dump(self, file)

    def clear(self):
        """
        Clear the snapshot of the queue.
        """
        with self._lock:
            path = self.generate_temp_path()
            if os.path.exists(os.path.join(path, "queue_snapshot")):
                os.remove(os.path.join(path, "queue_snapshot"))

    def restore(self):
        """
        Restore the snapshot of the queue and load it from a file.
        """
        with self._lock:
            path = self.generate_temp_path()

            if os.path.exists(os.path.join(path, "queue_snapshot")):
                print("Restoring snapshot...")
                with open(os.path.join(path, "queue_snapshot"), "rb") as file:
                    try:
                        data: StepQueue = pickle.load(file)
                        print(f"Restored {data}")
                        self.set_queues(data.get_queues())
                    except EOFError:
                        print("No snapshot found")
            else:
                print("No snapshot found")

    def get_queues(self):
        """
        Get the queues of the StepQueue object.

        Returns
        -------
        `list[PriorityQueue]`
            The queues of the StepQueue object.
        """
        return self._queues

    def set_queues(self, queues: list[PriorityQueue]):
        """
        Set the queues of the StepQueue object.

        Parameters
        ----------
        queues : `list[PriorityQueue]`
            The queues of the StepQueue object.
        """
        self._queues = queues

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['_lock']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._lock = threading.Lock()

    def __str__(self):
        payload = str()
        for i, queue in enumerate(self._queues):
            payload += f"Step {i}: {queue._queue}\n"
        return payload
