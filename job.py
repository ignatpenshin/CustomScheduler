class Job:
    def __init__(self, 
                 action: staticmethod,
                 start_at: str = "", 
                 max_working_time: int = -1, 
                 tries: int = 0, 
                 dependencies: list|None = None):
        if dependencies is None:
            self.dependencies = []
        elif isinstance(dependencies, list):
            self.dependencies = dependencies
        self.action = action
        self._start_at = start_at
        self._max_working_time = max_working_time
        self.tries = tries
        self._timelimit = 10
    
    @property
    def max_working_time(self):
        if self._max_working_time <= 0:
            return self._timelimit
        return self._max_working_time
    
    @max_working_time.setter
    def max_working_time(self, value: int):
        if value <= 0:
            self._max_working_time = self._timelimit
        else:
            self._max_working_time = value

    @property
    def start_at(self):
        return float(self._start_at)
    
    @start_at.setter
    def start_at(self, value: str):
        self._start_at = float(value)

    def run(self, *args):
        task = self.action(*args)
        while True:
            try:
                yield next(task)
            except StopIteration:
                yield None
                break
                

    def pause(self):
        pass

    def stop(self):
        pass