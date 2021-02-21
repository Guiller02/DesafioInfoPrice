# Imports
import findspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext


class Load:
    _final_dataframe = ''
    _spark_dataframe = ''
    _sql_c = ''
    _output_location = ''
    _page = ''
    _sc = ''

    def __init__(self, final_dataframe, output_location, page):
        self._final_dataframe = final_dataframe
        self._output_location = output_location
        self._page = page

    # Start conection with spark
    def _create_connection(self):
        print('Starting Load step')
        findspark.init()

        conf = SparkConf().setAppName('Scrapy_Projecst').setMaster('local')

        self._sc = SparkContext(conf=conf)

        self._sql_c = SQLContext(self._sc)

    # Create a spark dataframe
    def _create_spark_dataframe(self, dataframe) -> list:
        self._create_connection()
        return self._sql_c.createDataFrame(dataframe)

    # Write dataframe to csv (as i'm not using hadoop vm or anything, i will write to local hardware)
    def write_dataframe(self):
        self._spark_dataframe = self._create_spark_dataframe(self._final_dataframe)
        self._spark_dataframe.write.csv(self._output_location + 'scrapy_output/scrapy_' + str(self._page) + '_.csv',
                                        mode='overwrite')
        print('Finished Load step')

        # self._sc.stop()
        print('---------------------------------')


if __name__ == '__main__':
    import Scrapy

    sc = Scrapy.Scrapy('https://www.americanas.com.br/categoria/tv-e-home-theater/tv/pagina-1')

    sc.scrapy()

    lo = Load(sc.americanas_dataframe, '../../data/output/', 1)

    lo.write_dataframe()
