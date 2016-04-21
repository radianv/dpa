# coding: utf-8

import luigi

from luigi.contrib.spark import SparkSubmitTask, PySparkTask

import logging

logging.config.fileConfig('dpa_logging.conf')
logger = logging.getLogger('dpa.pipeline.test')

class HolaMundoTask(luigi.Task):
    """
    Una tarea sencilla y simple en python
    """
    task_namespace = 'test'

    def run(self):
        print("{task} says: ¡Hola Mundo desde Data Product Architecture!".format(task=self.__class__.__name__))


class HolaMundoSpark(PySparkTask):
    """
    Es posible tener pyspark adentro del código de luigi...
    """
    driver_memory = '2g'
    executor_memory = '3g'

    def main(self, sc, *args):
        logger.debug("Iniciando la tarea en spark")
        result = sc.parallelize(range(10000))\
                   .reduceByKey(lambda a, b: a + b)
        logger.debug("{result} es la suma".format(result=result))
