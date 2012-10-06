class Task:
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

    def __repr__(self):
        return "%s:%s runs=[%s]" % (self.mJobId, self.mIndex, self.mRuns)
