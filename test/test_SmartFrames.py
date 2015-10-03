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

import sys
import os
import unittest
src_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(src_dir, '..'))

from pyspark.sql.types import Row, StructType, StructField, IntegerType, StringType
from SparkTestCase import SparkTestCase
from smartframes import SmartFrames


class SimpleTable(SmartFrames.SmartFrames):
    schema = StructType( sorted(
        [
        StructField("pk_id", IntegerType()),
        StructField("first_name", StringType()),
        ],
        key = lambda x: x.name))
    skipSelectedFields = []


class TestSmartFrames(SparkTestCase):

    def testSimpleTable(self):
        simpleTable = SimpleTable()
        self.assertEquals(simpleTable.schema, SimpleTable().schema)

        s1 = SimpleTable()
        s1.pk_id = 1
        s1.first_name = 'Don'
                        
        s2 = SimpleTable()
        s2.pk_id = 2
        s2.first_name = 'Dan'
        df = self.sqlCtx.createDataFrame(self.sc.parallelize([s1.createRow(), s2.createRow()]), s1.schema)
        self.assertEquals(2, df.count())
        print "count=", df.count()

        fileName = self.tempdir + '/simple.table'
        df.saveAsParquetFile(fileName)

        df2 = self.sqlCtx.parquetFile(fileName)
        self.assertEquals(sorted(df.collect()), sorted(df2.collect()))


if __name__ == '__main__':
    unittest.main()
