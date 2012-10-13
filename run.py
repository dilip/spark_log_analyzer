import sys
import web

from Analyzer import Analyzer

analyzer = None

class DefaultHandler:        
    def GET(self):
        raise web.seeother('/static/index.html')

class DataHandler:
    def GET(self):
        return analyzer.toJSON()

class TestDataHandler:
    def GET(self):
        f = open("tests/data001.json","r")
        data = f.read()
        f.close() 
        return data

def startWebServer():
    URLS = (
        '/test/data.json', TestDataHandler,
        '/data.json', DataHandler,
        '/', 'DefaultHandler'
    )

    app = web.application(URLS, globals())
    # If command line arguments are specified, web.py assumes that it 
    # is the host and port.  Wierd!!
    # So need to jump through hoops to specify the port and use
    # command line arguments for something else.
    del sys.argv[:] # This is the python way to clear a list
    sys.argv.insert(0, "8080") 
    app.run()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "Usage: python run.py SPARK_LOG_FILE"
        sys.exit(-1)

    logFileName = sys.argv[1]
    analyzer = Analyzer()
    analyzer.processFile(logFileName)

    #print analyzer
    print analyzer.toJSON()
    
    print "Starting webserver"

    startWebServer()


