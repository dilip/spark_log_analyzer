import sys
import re
from datetime import datetime

from Job import Job

class Analyzer:
    """
    Analyze the log file produced at the Spark master, i.e., by the program that launches a Spark job.

    """

    SPARK_LOG_LINE_REGEX = re.compile(r"""(\d+)/(\d+)/(\d+) (\d\d):(\d\d):(\d\d) INFO (spark\..*)""")
    ADDING_JOB_REGEX = re.compile(r"""spark.MesosScheduler: Adding job with ID (\d+)""")

    def __init__(self):
        # JobId -> Job
        self.mJobs = {}


    def addJob(self, job):
        self.mJobs[job.id()] = job


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

        matchResult = Analyzer.ADDING_JOB_REGEX.match(logMsg)
        if matchResult:
            jobId = matchResult.group(1)
            self.addJob(Job(jobId, dt))
            return

    def __repr__(self):
        return str(self.mJobs)

