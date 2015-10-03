from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, HiveContext
import unittest
import tempfile
import shutil


# Drake
# helper class to setup Spark context for tests



class SparkTestCase(unittest.TestCase):

    def setUp(self):
        # setup a new spark context for each test
        conf = SparkConf()
        #conf.set("spark.driver.memory", "4g")
        conf.set("spark.cores.max", "1")
        conf.set("spark.master", "local[*]")
        conf.set("spark.app.name", "nosetests - smartframes")

        self.sc = SparkContext(conf=conf)
        self.sqlCtx = SQLContext(self.sc)

        self.tempdir = tempfile.mkdtemp()


    def tearDown(self):
        self.sc.stop()
        shutil.rmtree(self.tempdir)
