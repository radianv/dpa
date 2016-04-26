# -*- coding: utf-8 -*-

import re
import sys

import locale

import datetime

from random import random

from operator import add

from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import Row
from pyspark.conf import SparkConf

from pyspark.sql.functions import lag
from pyspark.sql import Window
from pyspark.sql import functions as F

if __name__ == "__main__":

    sc = SparkContext()
    sqlCtx = HiveContext(sc)

    print("Iniciando la tarea en spark")

    sqlCtx.read.format('com.databricks.spark.csv')\
                               .options(header='true', inferSchema='true', delimiter=',')\
                               .load(sys.argv[1])\
                               .registerTempTable('avistamientos')
    sqlCtx.sql("select State as estado, count(*) as avistamientos from avistamientos group by State")\
	      .coalesce(1)\	
              .write.parquet(sys.argv[2],mode='overwrite')

    print ("Terminando la tarea de Spark")

    sc.stop()
