#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function

import luigi
import luigi.contrib.spark

import datetime


class TestPySparkTask(luigi.contrib.spark.SparkSubmitTask):
    """
    Este task manda una tarea al Spark Master
    """
    @property
    def name(self):
        return 'TestPySpark'

    @property
    def app(self):
        return 'suma.py'

    @property
    def py_files(self):
        files = ['dpa_logging.conf']
        return files
