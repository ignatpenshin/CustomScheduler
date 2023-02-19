import random
import time
import pickle
import os
from types import MethodType
from inspect import stack
from datetime import datetime, timedelta

from task import Task
from job import Job


class Scheduler:
    def __init__(self, pool_size=3):
        self._startime = None
        self.history = None
        self.logfile = "logfile.pickle"
        self.pool = pool_size
        self.run()

    def generate_job(self):
        self._jobList = []
        self._jobInfo = {}
        self._IPC = {}
        self._taskSet = [i for i in Task.__dict__.keys() 
                            if not i.startswith("__")]

        self._startTime = datetime.timestamp(datetime.now())

        for i, task in enumerate(self._taskSet):
            self._jobList.append(
                Job(action = Task.__dict__[task], 
                    start_at = datetime.timestamp(datetime.now() + \
                               timedelta(milliseconds=random.randint(100, 500))),
                    max_working_time = random.randint(-1, 2), 
                    tries = random.randint(15, 40), 
                    dependencies = [Task.__dict__[job] for job 
                                    in self._taskSet[0:i] if i > 0]))

    def schedule(self, job):
        if job.action not in self._jobInfo:
            self._jobInfo[job.action] = \
               [job.start_at, 
                job.start_at + job.max_working_time, 
                0, 
                [False for i in range(len(job.dependencies))], 
                job.run,
                []]

        elif type(self._jobInfo[job.action]) != str:
            for i, jobDep in enumerate(job.dependencies):
                if jobDep in self._jobInfo and self._jobInfo[jobDep] == "Done":
                    self._jobInfo[job.action][3][i] = True

        # Run condition
        if (set(self._jobInfo[job.action][3]) == {True, } or
            self._jobInfo[job.action][3] == []) and \
            self._jobInfo[job.action][2] <= job.tries and \
            self._jobInfo[job.action][1] >= datetime.timestamp(datetime.now()) >= self._jobInfo[job.action][0]:

            # Go Do Your Job!
            print("job " + job.action.__name__ + \
                  " in " + str(self._jobInfo[job.action][2]) + \
                  " tries. Call:")

            if isinstance(self._jobInfo[job.action][4], MethodType):
                if len(job.dependencies) > 0:
                    args = [self._IPC[job.dependencies[-1].__name__],]
                    worker = self._jobInfo[job.action][4](*args)
                else: 
                    args = []
                    worker = self._jobInfo[job.action][4](*args)
                self._jobInfo[job.action][4] = worker
                self._jobInfo[job.action][5] = args

            value = next(self._jobInfo[job.action][4])      
            if value is not None:
                if job.action.__name__ not in self._IPC:
                    self._IPC[job.action.__name__] = []  
                self._IPC[job.action.__name__].append(value)
            else:
                if len(self._jobInfo[job.action][5]) > 0:
                    del self._IPC[job.dependencies[-1].__name__]
                self._jobInfo[job.action] = "Done"
                self._jobList.remove(job)

        if isinstance(self._jobInfo[job.action], list) and \
            datetime.timestamp(datetime.now()) > self._jobInfo[job.action][1]:
            if job.tries == self._jobInfo[job.action][2]:
                self._jobInfo[job.action] = "Failed"
                print("job " + job.action.__name__ + " failed. ")
                self.stop()
            
            self._jobInfo[job.action][2] += 1
            self._jobInfo[job.action][1] = datetime.timestamp(datetime.now() + \
                                           timedelta(seconds = job.max_working_time)) 
        if len(self._IPC):
            print(self._IPC)
 
    def run(self):
        self.generate_job()

        if os.path.isfile(self.logfile):
            self.restart()

        while len(self._jobList) > 0:
            deq = self._jobList[:self.pool]
            for job in deq:
                self.schedule(job)
            time.sleep(0.1)
            if random.randint(0,5) == 3:
                self.stop()  
        self.stop()
               
    def restart(self):
        print("Restarting")

        with open(self.logfile, 'rb') as handle:
            self.history = pickle.load(handle)
            self._IPC = self.history["IPC"]

            timingDelta = self._startTime - self.history["start_time"]

            for job in self._jobList:
                try:
                    job_log = self.history[job.action.__name__]
                    if isinstance(job_log, list):
                        job.start_at = job_log[0] + timingDelta
                        job.max_working_time = int(job_log[1] - job_log[0])
                        self._jobInfo[job.action] = [job.start_at,
                                                     job.start_at + \
                                                     job.max_working_time,
                                                     job_log[2], 
                                                     job_log[3],
                                                     job.run,
                                                     []]
                    elif isinstance(job_log, str):
                        self._jobInfo[job.action] = job_log
                except KeyError:
                    continue
        
        job_control = [type(i[1]) for i in self._jobInfo.items()]
        if set(job_control) == {str,}:
            self.stop()

    def stop(self):
        if stack()[1].function == "schedule":
            try:
                os.remove(self.logfile) 
            except OSError:
                pass
            exit("Failed. Scheduler stopped. JobList cleaned.")

        elif stack()[1].function == "run" and len(self._jobList) == 0:  
            try:
                os.remove(self.logfile) 
            except OSError:
                pass 
            exit("Finished good") 

        elif stack()[1].function == "run" and len(self._jobList) > 0:
            print("Stopped. Logging results and restart")
            pickle_dict = {}
            for job in self._jobInfo.keys():
                pickle_dict[job.__name__] = self._jobInfo[job][:4]
            pickle_dict["start_time"] = self._startTime
            pickle_dict["IPC"] = self._IPC

            with open(self.logfile, 'wb') as handle:
                pickle.dump(pickle_dict, 
                            handle, 
                            protocol=pickle.HIGHEST_PROTOCOL)
            print("Run with logged data.")
            self.run()
        
        elif stack()[1].function == "restart":
            try:
                os.remove(self.logfile) 
            except OSError:
                pass
            exit("Finished good. Exit from recursion.")

if __name__ == "__main__":
    Scheduler()
