# Copyright 2015 Don Drake don@drakeconsulting.com
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
