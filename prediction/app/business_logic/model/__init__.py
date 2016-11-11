import pandas as pd
import logging

rootLogger = logging.getLogger()
rootLogger.setLevel(logging.INFO)
logFormatter = logging.Formatter(
    "%(asctime)s [%(levelname)-5.5s]  %(message)s")

fileHandler = logging.FileHandler("{0}/{1}.log".format("app/business_logic/logs", "froggy"))
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)
