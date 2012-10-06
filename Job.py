class Job:
    """ 
    Represents a Spark Job
    
    """

    def __init__(self, id, startDt):
        # JobId
        self.mId = id
        # Time at which job was started.
        self.mStartDt = startDt

    def id(self):
        return self.mId 

    def __repr__(self):
        return "JobId=%s StartedAt=%s" % (self.mId, self.mStartDt)
     
