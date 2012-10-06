class Job:
    """ 
    Represents a Spark Job
    
    """

    def __init__(self, id, startDt):
        # JobId
        self.mId = id

        # Time at which job was started.
        self.mStartDt = startDt
        
        # Task.index() -> Task
        self.mTasks = {}


    def id(self):
        return self.mId 


    def addTask(self, task):
        self.mTasks[task.index()] = task


    def getTask(self, index):
        return self.mTasks.get(index, None)


    def __repr__(self):
        return "JobId=%s StartedAt=%s Tasks=[%s]" % (self.mId, self.mStartDt, self.mTasks)
     
