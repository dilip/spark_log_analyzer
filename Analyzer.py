import sys
import re
from datetime import datetime

from Job import Job
from Task import Task
from TaskRun import TaskRun

class Analyzer:
    """
    Analyze the log file produced at the Spark master, i.e., by the program that launches a Spark job.

    """

    SPARK_LOG_LINE_REGEX = re.compile(r"""(\d+)/(\d+)/(\d+) (\d\d):(\d\d):(\d\d) INFO (spark\..*)""")
    ADDING_JOB_REGEX = re.compile(r"""spark.MesosScheduler: Adding job with ID (\d+)""")
    STARTING_TASK_REGEX = re.compile(r"""spark.SimpleJob: Starting task (\d+):(\d+) as TID (\d+) on slave (\S+): (\S+) (\S+)""")
    FINISHED_TID_REGEX = re.compile(r"""spark.SimpleJob: Finished TID (\d+)""")

    def __init__(self):
        # JobId -> Job
        self.mJobs = {}

        # TID -> TaskRun
        self.mTaskRuns = {}


    def addJob(self, job):
        self.mJobs[job.id()] = job

    def getJob(self, id):
        return self.mJobs.get(id, None)

    def addTaskRun(self, taskRun):
        self.mTaskRuns[taskRun.tid()] = taskRun

    def getTaskRun(self, tid):
        return self.mTaskRuns.get(tid, None)

    def processFile(self, filePath):
        print "Processing %s" % filePath

        logFile = open(filePath, "r")
        for line in logFile:
            self.processLine(line.strip()) 
        logFile.close()

 
    def processLine(self, line):
        """
        Process the specified log line.  If it is a spark log line, we appropriately
        further process it.
        """

        matchResult = Analyzer.SPARK_LOG_LINE_REGEX.match(line)
        if matchResult:
            dt = datetime(  2000 + int(matchResult.group(1)), 
                        int(matchResult.group(2)), 
                        int(matchResult.group(3)), 
                        int(matchResult.group(4)), 
                        int(matchResult.group(5)), 
                        int(matchResult.group(6)))
            logMsg = matchResult.group(7) 
            self.processSparkLog(dt, logMsg)


    def processSparkLog(self, dt, logMsg):
        """
        Process the specified spark logMsg.
        dt : datetime (i.e., timestamp) associated with the logMsg
        """

        matchResult = Analyzer.STARTING_TASK_REGEX.match(logMsg)
        if matchResult:
            jobId = matchResult.group(1)
            index = matchResult.group(2)
            job = self.getJob(jobId)
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
            self.addTaskRun(taskRun)
            return

        matchResult = Analyzer.ADDING_JOB_REGEX.match(logMsg)
        if matchResult:
            jobId = matchResult.group(1)
            self.addJob(Job(jobId, dt))
            return


        matchResult = Analyzer.FINISHED_TID_REGEX.match(logMsg)
        if matchResult:
            tid = matchResult.group(1)
            taskRun = self.getTaskRun(tid)
            if taskRun is None:
                print "ERROR: Could not find TaskRun for TID %s which just finished" % tid
            else:
                taskRun.setEndDt(dt)
            return
            

    def __repr__(self):
        return str(self.mJobs)

