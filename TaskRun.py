from Utils import dt2Epoch

class TaskRun:
    """
    Represents a run of a Task.  Normally a Task runs only once.
    However, if there is an error, it can run multiple times.
    
    """

    def __init__(self, tid, startDt, slaveId, slaveHost):
        self.mTid = tid
        self.mStartDt = startDt
        self.mEndDt = None
        self.mSlaveId = slaveId
        self.mSlaveHost = slaveHost

    def tid(self):
        return self.mTid

    def setEndDt(self, dt):
        self.mEndDt = dt

    def jsonDict(self):
        return {
                "tid": self.tid(),
                "startEpochSeconds": dt2Epoch(self.mStartDt),
                "endEpochSeconds": dt2Epoch(self.mEndDt)
            }

    def __repr__(self):
        return "%s on %s|%s (%s-%s)" % (self.tid(), self.mSlaveId, self.mSlaveHost, 
                        self.mStartDt, self.mEndDt)
