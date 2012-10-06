import sys

from Analyzer import Analyzer

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "Usage: python run.py SPARK_LOG_FILE"
        sys.exit(-1)

    logFileName = sys.argv[1]
    analyzer = Analyzer()
    analyzer.processFile(logFileName)

    print analyzer

