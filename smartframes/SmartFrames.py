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

    _identity = lambda val: val

    _String = lambda val: str(val) if val is not None else None

    def _Integer(val):
        #print "_Integer val=", val
        if val == '':
            val = None
        val = int(val) if val is not None else None
        return val

    def _Date(val):
        if isinstance(val, str):
            val = date.strptime(val, '%Y-%m-%d')
        elif isinstance(val, datetime):
            val = val.date()
        elif isinstance(val, date):
            pass
        else:
            raise Exception("cant convert to date:" + val)
        return val

    _Float = lambda(val): float(val)

    def _Long(val):
        if val == '':
            val = None
        val = long(val) if val is not None else None
        return val

    _dispatchTable = {
        StringType(): _String,
        IntegerType(): _Integer,
        TimestampType(): _identity,
        DateType(): _Date,
        DoubleType(): _Float,
        FloatType(): _Float,
        ShortType(): _Integer,
        ByteType(): _Integer,
        LongType(): _Long,
        BooleanType(): _identity,
        BinaryType(): _identity,
    }

    def __init__(self):
        # initialize object values to be None
        for _structType in self.schema.fields:
            self.__dict__[_structType.name] = None

    def createRow(self):
        d = {}
        for _structType in sorted(self.schema.fields):
            #print "_structType=", _structType
            val = self.__dict__[_structType.name]
            try:
                func = self._dispatchTable[_structType.dataType]
                val = func(val)
            except KeyError:
                print "Not found in dispatch ", _structType.dataType
                print "val=", val
                pass
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

