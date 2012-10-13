import sys
import re
import json
from datetime import datetime

from Job import Job
from Task import Task

class Analyzer:
    """
    Analyze the log file produced at the Spark master, i.e., by the program that launches a Spark job.

    """

    SPARK_LOG_LINE_REGEX = re.compile(r"""(\d+)/(\d+)/(\d+) (\d\d):(\d\d):(\d\d) INFO (spark\..*)""")

    def __init__(self):
        # JobId -> Job
        self.mJobsDict = {}
        
        self.mJobs = []

        # TID -> TaskRun
        self.mTaskRuns = {}


    def addJob(self, job):
        self.mJobsDict[job.id()] = job
        self.mJobs.append(job)

    def setEndDtOfLastJob(self, dt):
        if len(self.mJobs) > 0:
            self.mJobs[-1].setEndDtIfNotSet(dt)

    def getJob(self, id):
        return self.mJobsDict.get(id, None)

    def addTaskRun(self, taskRun):
        self.mTaskRuns[taskRun.tid()] = taskRun

    def getTaskRun(self, tid):
        return self.mTaskRuns.get(tid, None)

    def processFile(self, filePath):
        print "Processing %s" % filePath

        logFile = open(filePath, "r")
        lastDt = None
        for line in logFile:
            dt = self.processLine(line.strip())  
            if dt is not None:
                lastDt = dt # Also check if actual timestamp value is greater
        if lastDt is not None:
            self.setEndDtOfLastJob(lastDt)
        logFile.close()

 
    def processLine(self, line):
        """
        Process the specified log line.  If it is a spark log line, we appropriately
        further process it.
        Returns the timestamp of the log line if it is a spark log.  Else returns None
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
            return dt

        return None


    def processSparkLog(self, dt, logMsg):
        """
        Process the specified spark logMsg.
        dt : datetime (i.e., timestamp) associated with the logMsg
        """

        if Task.processSparkLog(self, dt, logMsg):
            return

        if Job.processSparkLog(self, dt, logMsg):
            return
           

    def __repr__(self):
        return str(self.mJobs)

    def toJSON(self):
        jobs = [job.jsonDict() for job in self.mJobs]
        return json.dumps(jobs)

