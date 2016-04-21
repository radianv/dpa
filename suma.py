# -*- coding: utf-8 -*-

from __future__ import print_function

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

import logging
#logging.config.fileConfig('dpa_logging.conf')
logger = logging.getLogger('dpa.pipeline.test')


if __name__ == "__main__":

    def f(x):
        x = random() * x
        return x

    sc = SparkContext(appName="PiPySpark")

    conf = SparkConf()

    print(conf.getAll())
    print(sc.version)
    print(sc)
    #print(sys.argv[1])
    #print(sys.argv[2])

    #sqlCtx = HiveContext(sc)

    print("Iniciando la tarea en spark")
    result = sc.parallelize(range(10000))\
               .map(f)\
               .reduce(add)

    print("{result} es nuestra cálculo".format(result=result))

    sc.stop()
