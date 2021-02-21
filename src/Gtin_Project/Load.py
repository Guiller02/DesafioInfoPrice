# Imports
import findspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext


class Load:
    _final_dataframe = ''
    _spark_dataframe = ''
    _sql_c = ''
    _output_location = ''

    def __init__(self, final_dataframe, output_location):
        # receive the final dataframe from transform step and the output location
        self._final_dataframe = final_dataframe
        self._output_location = output_location

    # Start conection with spark
    def _create_connection(self):
        print('Starting Load step')
        findspark.init()

        conf = SparkConf().setAppName('Gtin_Project').setMaster('local')

        sc = SparkContext(conf=conf)

        self._sql_c = SQLContext(sc)

    # Create a spark dataframe
    def _create_spark_dataframe(self, dataframe) -> list:
        self._create_connection()
        return self._sql_c.createDataFrame(dataframe)

    # Write dataframe to csv (as i'm not using hadoop vm or anything, i will write to local hardware)
    def write_dataframe(self):
        self._spark_dataframe = self._create_spark_dataframe(self._final_dataframe)
        self._spark_dataframe.write.csv(self._output_location + 'Gtin_output/gtin.csv', mode='overwrite')
        print('Finished Load step')
        print('---------------------------------')


# to test the file
if __name__ == '__main__':
    from src.Gtin_Project import Transform, Extract

    ex = Extract.Extract('../../data/input/')

    ex.extract_bases()

    tr = Transform.Transform(
        ex.dataframe_info_mix
        , ex.dataframe_descricoes
        , ex.dataframe_gs1
        , ex.dataframe_cnpj
        , ex.dataframe_cosmos
    )

    tr.transform_dataframes()

    lo = Load(tr.final_dataframe, '../../data/output/')

    lo.write_dataframe()
