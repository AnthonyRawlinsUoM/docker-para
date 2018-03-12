from multiprocessing import Pool, TimeoutError
import time
import os
import queue


class ProcessQueue:
    
    def __init__(self):
        self.priority_queue = queue.PriorityQueue()
        
    def loop(self):
        """ Main processing loop """
        with Pool(processes=4) as pool:
            pool.map(worker)
    
    
    def worker():
        while True:
            item = priority_queue.get_nowait()
            if item is None:
                break
            do_work(item)
            priority_queue.task_done() 
    
    def do_work(self, item):
        
    
    def add_to_queue(self, task: Task, callback=None):
        

if __name__ == '__main__':
    
