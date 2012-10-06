class TaskRun:
    """
    Represents a run of a Task.  Normally a Task runs only once.
    However, if there is an error, it can run multiple times.
    
    """

    def __init__(self, tid, startDt, slaveId, slaveHost):
        self.mTid = tid
        self.mStartDt = startDt
        self.mSlaveId = slaveId
        self.mSlaveHost = slaveHost

    def __repr__(self):
        return "%s on %s|%s" % (self.mTid, self.mSlaveId, self.mSlaveHost)
