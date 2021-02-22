from src.Gtin_Project import Transform, Extract, Load
from src.Scrap_project import Scrapy
from time import sleep

# The main file contains both project, but if you want, you can run each one individually, running
# Load file on Gtin_Project and Scrapy in Scrap_project

# Gtin project

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

# Scrapy project

print('-------------')

print('Starting scrapy project')

sc = Scrapy.Scrapy('https://www.americanas.com.br/categoria/tv-e-home-theater/tv/pagina-', '../data/output/')

sc.scrapy()

print('Finished Scrapy project')
