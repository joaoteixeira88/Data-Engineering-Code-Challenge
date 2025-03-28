from pyspark.sql import SparkSession

from src.crosscutting.constants import GeneralConstants


class SparkSingleton:
    """
    Singleton class for SparkSession.
    Ensures that only one SparkSession is created and reused throughout the application.
    """

    _instance = None

    @staticmethod
    def get_instance():
        """
        Returns the singleton instance of SparkSession.
        If it doesn't exist, creates a new one.
        """
        if SparkSingleton._instance is None:
            SparkSingleton._instance = SparkSession.builder.appName(
                GeneralConstants.SPARK_SESSION
            ).getOrCreate()
        return SparkSingleton._instance


spark = SparkSingleton.get_instance()
