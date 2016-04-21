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

    sqlCtx.read.parquet(sys.argv[1])\
               .registerTempTable('avistamientos_por_estado')

    sqlCtx.sql("select estado, avistamientos from avistamientos_por_estado"
               " order by avistamientos desc limit 10 ")\
          .write.json(sys.argv[2], mode='overwrite')

    print("terminando la tarea en Spark")

    sc.stop()
