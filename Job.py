import re

from Utils import dt2Epoch

class Job:
    """ 
    Represents a Spark Job
    
    """

    END_JOB_REGEX = re.compile(r"""spark.MesosScheduler: Got a job with \d+ tasks""")
    ADDING_JOB_REGEX = re.compile(r"""spark.MesosScheduler: Adding job with ID (\d+)""")

    def __init__(self, id, startDt):
        # JobId
        self.mId = id

        # Time at which job was started.
        self.mStartDt = startDt

        self.mEndDt = None
        
        # Task.index() -> Task
        self.mTasks = {}

    def setEndDtIfNotSet(self, dt):
        print "Foo", dt
        if self.mEndDt is None:
            self.mEndDt = dt

    def id(self):
        return self.mId 


    def addTask(self, task):
        self.mTasks[task.index()] = task


    def getTask(self, index):
        return self.mTasks.get(index, None)


    def __repr__(self):
        return "JobId=%s StartedAt=%s EndedAt=%s Tasks=[%s]" % (self.mId, self.mStartDt, self.mEndDt, self.mTasks)
        #return "JobId=%s StartedAt=%s EndedAt=%s" % (self.mId, self.mStartDt, self.mEndDt)

    def jsonDict(self):
        return {
                "id": self.id(),
                "startEpochSeconds": dt2Epoch(self.mStartDt),
                "endEpochSeconds": dt2Epoch(self.mEndDt),
                "tasks": [task.jsonDict() for task in self.mTasks.values()]
            }


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

        matchResult = Job.END_JOB_REGEX.match(logMsg)
        if matchResult:
            analyzer.setEndDtOfLastJob(dt)
            return True

        return False

