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


from datetime import date, datetime
from pyspark.sql.types import Row, StructType, StructField, IntegerType, StringType, TimestampType, DateType, DoubleType, ShortType, ByteType, BooleanType, BinaryType, FloatType, LongType

# Don Drake 
# don@drakeconsulting.com

class SmartFrames(object):
    schema = None
    skipSelectFields = []

    def __init__(self):
        # initialize object values to be None
        for _structType in self.schema.fields:
            self.__dict__[_structType.name] = None

    def createRow(self):
        d = {}
        for _structType in sorted(self.schema.fields):
            #print "_structType=", _structType
            val = self.__dict__[_structType.name]
            #print "val=", val
            if _structType.dataType == StringType():
                val = str(val) if val is not None else None
                #print "now String"
            elif _structType.dataType == IntegerType():
                if val == '':
                    val = None
                val = int(val) if val is not None else None
                #print "now Int", val, "name=", _structType.name
            elif _structType.dataType == TimestampType():
                pass
            elif _structType.dataType == DateType():
                if isinstance(val, str):
                    val = date.strptime(val, '%Y-%m-%d')
                elif isinstance(val, datetime):
                    val = val.date()
                elif isinstance(val, date):
                    pass
                else:
                    raise Exception("cant convert to date:" + val)
            elif _structType.dataType == DoubleType():
                val = float(val)
            elif _structType.dataType == FloatType():
                val = float(val)
            elif _structType.dataType == ShortType():
                val = int(val)
            elif _structType.dataType == ByteType():
                val = int(val)
            elif _structType.dataType == LongType():
                val = long(val)
            elif _structType.dataType == BooleanType():
                pass
            elif _structType.dataType == BinaryType():
                pass
            else:
                print "TYPE NOT FOUND, " + str(_structType) 
            d[_structType.name] = val
            
        #print "CONVERTED, d=", d
        return Row(**d)

    def getSelectFields(self, df):
        cols = []
        for field in self.schema.fields:
            fieldname = field.name
            if fieldname in self.skipSelectFields:
                continue
            cols.append(df[fieldname])
        return cols

    def getSelectFieldNames(self, tableAlias):
        cols = []
        for field in self.schema.fields:
            fieldname = field.name
            if fieldname in self.skipSelectFields:
                continue
            cols.append(tableAlias + "." + fieldname)
        return ", ".join(cols)

    def __repr__(self):
        return str(self.__dict__)

    def __str__(self):
        return str(self.__dict__)

