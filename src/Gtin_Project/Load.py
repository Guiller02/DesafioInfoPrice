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

    # Write dataframe to csv (as i'm not using hadoop vm or anything, i will write to local hardware)
    def write_dataframe(self):
        self._final_dataframe.to_csv(self._output_location + 'Gtin_output/gtin_data.csv', sep=';', index=False)

        print('Finished Load')
        # self._sc.stop()
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
