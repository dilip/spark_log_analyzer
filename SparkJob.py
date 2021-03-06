import re

from Utils import dt2Epoch

class SparkJob:
    """ 
    Represents a Spark Job.  A Spark job can contain multiple mesos jobs.
    
    """

    STARTING_JOB_REGEX = re.compile(r"""spark.SparkContext: Starting job...""")
    FINISHED_JOB_REGEX = re.compile(r"""spark.SparkContext: Job finished in (\S+) s""")

    def __init__(self, startDt):
        # Time at which job was started.
        self.mStartDt = startDt

        self.mEndDt = None

        # Duration for spark job directly read from log line, and not calculated from mEndDt and mStartDt.
        self.mDurationSeconds = None
        
        self.mMesosJobs = []

    def setEndDtIfNotSet(self, dt):
        if self.mEndDt is None:
            self.mEndDt = dt

    def setDuration(self, durationSeconds):
        self.mDurationSeconds = durationSeconds

    def addMesosJob(self, mesosJob):
        self.mMesosJobs.append(mesosJob)

    def getLastMesosJob(self):
        if len(self.mMesosJobs) > 0:
            return self.mMesosJobs[-1]
        else:
            return None
            # We don't raise an exception because the last mesos job could sometimes 
            # be not defined because we using the start of the next mesos job to identify
            # the end of the current mesos job

    def __repr__(self):
        #return "SparkJob StartedAt=%s EndedAt=%s Duration=%s s MesosJobs=[%s]" % (self.mStartDt, self.mEndDt, self.mDurationSeconds, self.mMesosJobs)
        return "SparkJob StartedAt=%s EndedAt=%s Duration=%s s" % (self.mStartDt, self.mEndDt, self.mDurationSeconds)


    def jsonDict(self):
        return {
                "startEpochSeconds": dt2Epoch(self.mStartDt),
                "endEpochSeconds": dt2Epoch(self.mEndDt),
                "durationSeconds": self.mDurationSeconds,
                "mesosJobs": [mesosJob.jsonDict() for mesosJob in self.mMesosJobs]
            }


    @staticmethod
    def processSparkLog(analyzer, dt, logMsg):
        """
        Return True if the logMsg was successfully processed, and no further processing is necessary.

        """

        matchResult = SparkJob.STARTING_JOB_REGEX.match(logMsg)
        if matchResult:
            analyzer.addSparkJob(SparkJob(dt))
            return True

        matchResult = SparkJob.FINISHED_JOB_REGEX.match(logMsg)
        if matchResult:
            analyzer.setEndDtOfLastMesosJob(dt)
            analyzer.setEndDtOfLastSparkJob(dt)
            analyzer.getLastSparkJob().setDuration(float(matchResult.group(1)))
            return True

        return False

