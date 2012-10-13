import re
from TaskRun import TaskRun

class Task:

    STARTING_TASK_REGEX = re.compile(r"""spark.SimpleJob: Starting task (\d+):(\d+) as TID (\d+) on slave (\S+): (\S+) (\S+)""")
    FINISHED_TID_REGEX = re.compile(r"""spark.SimpleJob: Finished TID (\d+)""")

    def __init__(self, jobId, index):
        """
        jobId:index uniquely identifies a task.

        """

        self.mJobId = jobId
        self.mIndex = index

        # TID -> TaskRun
        self.mRuns = {}


    def index(self):
        return self.mIndex

    def addRun(self, taskRun):
        self.mRuns[taskRun.mTid] = taskRun

    def jsonDict(self):
        return {
                "index": self.mIndex,
                "runs": [taskRun.jsonDict() for taskRun in self.mRuns.values()]
            }

    def __repr__(self):
        return "%s:%s runs=[%s]" % (self.mJobId, self.mIndex, self.mRuns)

    @staticmethod
    def processSparkLog(analyzer, dt, logMsg):
        """
        Return True if this logMsg was processed, and hence no further 
        processing is necessary.

        """

        matchResult = Task.STARTING_TASK_REGEX.match(logMsg)
        if matchResult:
            jobId = matchResult.group(1)
            index = matchResult.group(2)
            job = analyzer.getJob(jobId)
            if job is None:
                print "ERROR: Could not find job id = %s" % jobId
                return
            task = job.getTask(index)
            if task is None:
                task = Task(jobId, index)
                job.addTask(task)

            taskRun = TaskRun(matchResult.group(3), dt, 
                        matchResult.group(4), matchResult.group(5))
            task.addRun(taskRun)
            analyzer.addTaskRun(taskRun)
            return True


        matchResult = Task.FINISHED_TID_REGEX.match(logMsg)
        if matchResult:
            tid = matchResult.group(1)
            taskRun = analyzer.getTaskRun(tid)
            if taskRun is None:
                print "ERROR: Could not find TaskRun for TID %s which just finished" % tid
            else:
                taskRun.setEndDt(dt)
            return True
 
        return False

