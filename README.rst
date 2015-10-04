smartframes
===========
Enhanced Python Dataframes for Spark/PySpark

Motivation
----------

Spark DataFrames provide a nice interface to datasets that have a schema.  Getting data from your code into a DataFrame in Python means creating a Row() object with field names and respective values.  Given that you already have a schema with data types per field, it would be nice to easily take an object that represents the row and create the Row() object automatically.

This was written when creating Row()'s with Long datatypes and finding that Spark did not handle converting integers as longs when passing values to the JVM.  I needed a consistent manner to create Row() for all of my DataFrames.

The Row() object also allows you to reference the fields by name (e.g. myRow.firstName), this allows you to create an object that can now be transformed into a Row() object.

Installation
------------

.. code:: bash
    pip install smartframes


Example
-------

Simply create a class that extends from SmartFrame and define the schema as a sorted list of StructFields.  It's important that the schema is sorted as Spark gets upset if the Row() object is created with fields that are in a different order. Strange, but true.

The skipSelectedFields is a list of field names that you normally would not select when creating a select() statement. 

.. code:: python

    class SimpleTable(SmartFrames):
        schema = StructType( sorted(
            [
            StructField("pk_id", IntegerType()),
            StructField("first_name", StringType()),
            ],
            key = lambda x: x.name))
        skipSelectedFields = []

    ...

            s1 = SimpleTable()
            s1.pk_id = 1
            s1.first_name = 'Don'

            s2 = SimpleTable()
            s2.pk_id = 2
            s2.first_name = 'Dan'
            df = self.sqlCtx.createDataFrame(self.sc.parallelize([s1.createRow(), s2.createRow()]), s1.schema)


