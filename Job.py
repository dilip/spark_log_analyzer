import re

class Job:
    """ 
    Represents a Spark Job
    
    """

    ADDING_JOB_REGEX = re.compile(r"""spark.MesosScheduler: Adding job with ID (\d+)""")

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

    @staticmethod
    def processSparkLog(analyzer, dt, logMsg):
        """
        Return True if the logMsg was successfully processed, and no further processing is necessary.

        """

        matchResult = Job.ADDING_JOB_REGEX.match(logMsg)
        if matchResult:
            jobId = matchResult.group(1)
            analyzer.addJob(Job(jobId, dt))
            return True
        else:
            return False

