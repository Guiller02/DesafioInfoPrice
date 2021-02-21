from src.Gtin_Project import Transform, Extract, Load
from src.Scrap_project import Scrapy, Load as Loa
from time import sleep

print('Starting Gtin project ')
ex = Extract.Extract('../data/input/')

ex.extract_bases()

tr = Transform.Transform(
    ex.dataframe_info_mix
    , ex.dataframe_descricoes
    , ex.dataframe_gs1
    , ex.dataframe_cnpj
    , ex.dataframe_cosmos
)

tr.transform_dataframes()

lo = Load.Load(tr.final_dataframe, '../data/output/')

lo.write_dataframe()

print('Finished gtin project')

print('-------------')

print('Starting scrapy project')

for i in range(1, 5):
    sc = Scrapy.Scrapy('https://www.americanas.com.br/categoria/tv-e-home-theater/tv/pagina-' + str(i))

    sc.scrapy()

    loa = Loa.Load(sc.americanas_dataframe, '../data/output/', i)

    loa.write_dataframe()
    sleep(5000)
print('Finished Scrapy project')
