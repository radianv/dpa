# coding: utf-8

import os

import json

import datetime

import logging
logging.config.fileConfig('dpa_logging.conf')
logger = logging.getLogger('dpa.pipeline.etl')

import luigi
from luigi import configuration, LocalTarget
from luigi.s3 import S3Target, S3Client, S3FlagTarget, ReadableS3File
from luigi.contrib.spark import SparkSubmitTask, PySparkTask
import luigi.postgres


from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import Row
from pyspark.conf import SparkConf

from test import HolaMundoTask
import test_spark

class AllTasks(luigi.WrapperTask):
    """
    Las WrapperTask ahorran el método output()
    Si se usa una clase normal, el pipeline siempre marcará error
    """
    sighting_date = luigi.DateParameter(default=datetime.date.today())
    
    def requires(self):
        #yield test.HolaMundoTask()
        #yield test_spark.TestPySparkTask()
        yield TopStatesToDatabase()

class ReadUFOs(luigi.ExternalTask):
    def output(self):
        return luigi.s3.S3Target('s3n://itam-mcd/ufo/raw/')

class CleanUFODataSet(luigi.Task):
    pass

class DeduplicateUFOSightings(luigi.Task):
    pass

class AggregateUFOsByState(SparkSubmitTask):
    #date = luigi.DateParameter()
    
    sighting_date = luigi.DateParameter()    
    bucket = configuration.get_config().get('etl','bucket')    

    def requires(self):
        return ReadUFOs()

    @property
    def name(self):
        return 'AggregateUFOsByState'

    def app_options(self):
        return [self.input().path, self.output().path]

    @property
    def app(self):
        return 'aggregate_by_state.py'

    def output(self):
        #return luigi.s3.S3Target('s3://itam-mcd/ufo/etl/aggregated')
        return luigi.s3.S3Target('{}/ufo/etl/aggregated/year={}/month={}/day={}'.format(self.bucket,
									self.sighting_date.year,
									self.sighting_date.month,
									self.sighting_date.day))

class Top10States(SparkSubmitTask):
    #date = luigi.DateParameter()
    sighting_date = luigi.DateParameter()


    def requires(self):
        return AggregateUFOsByState(sighting_date = self.sighting_date)#date=self.date

    @property
    def name(self):
        return 'Top_10_States'

    def app_options(self):
        return [self.input().path, self.output().path]

    @property
    def app(self):
        return 'top10States.py'

    def output(self):
        return luigi.s3.S3Target('s3://itam-mcd/ufo/etl/top10/year={}/month={}'.format(
            self.date.year,
            self.date.month))

class TopStatesToDatabase(luigi.postgres.CopyToTable):
    
    host = configuration.get_config().get('dpadb','host')
    database = configuration.get_config().get('dpadb','database')
    user = configuration.get_config().get('dpadb','user')
    password = configuration.get_config().get('dpadb','password')
    table = configuration.get_config().get('etl','top10_table')

    columns = [('mes', 'TEXT'),
               ('estado', 'TEXT'),
               ('avistamientos', 'INT')]

    def requires(self):
        return Top10States()


if __name__ == '__main__':
    luigi.run()
