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

    avistamientos = sqlCtx.read.format('com.databricks.spark.csv')\
                               .options(header='true', inferSchema='true', delimiter='\t')\
                               .load(sys.argv[1])

    print(avistamientos.printSchema())

    avistamientos.write.json(sys.argv[2])

    print("Resultados guardados")

    sc.stop()
