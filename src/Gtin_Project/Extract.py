# The first step of ETl, the Extract

# Imports


import pandas as pd

import json

# Configure pandas to display all columns in dataframe
pd.options.display.max_columns = 500


class Extract:
    # Directory of the folder that contains all data
    _folder = ''

    # Array of data
    _gs1_data = []
    _cnpj_data = []
    _cosmos_data = []

    # Dataframes
    dataframe_info_mix = ''
    dataframe_descricoes = ''
    dataframe_gs1 = ''
    dataframe_cnpj = ''
    dataframe_cosmos = ''

    # This class receive the folder directory of the data files as a parameter and then assign to _folder
    def __init__(self, folder_directory) -> None:
        self._folder = folder_directory

    # The principal method (the only one public method), him will call all other methods to run
    def extract_bases(self) -> None:
        print('Starting of Extract step')
        self._extract_info_mix_data()
        self._extract_descricoes_externas_data_data()
        self._extract_gs1_data()
        self._extract_cnpj_data()
        self._extract_cosmos_data()
        print('Finishing Extract step')
        print('---------------------------------')

    # This method, receive the data array that will be transformed into a dataframe, and then return the dataframe
    def _generate_dataframe(self, data_array) -> list:
        _ = pd.DataFrame(data_array, dtype=str)
        _.columns = ['json_element']
        _['json_element'].apply(json.loads)
        return pd.json_normalize(_['json_element'].apply(json.loads))

    # because the file is a tab-separated  value, this two next methods does not need to call the above method
    def _extract_info_mix_data(self) -> None:
        self.dataframe_info_mix = pd.read_csv(self._folder + 'infomix.tsv', delimiter='\t', dtype=str)

    def _extract_descricoes_externas_data_data(self) -> None:
        self.dataframe_descricoes = pd.read_csv(self._folder + 'descricoes_externas.tsv', delimiter='\t', dtype=str)

    # This nexts methods, needs to call the generate_dataframe method, to create a dataframe
    def _extract_gs1_data(self) -> None:
        with open(self._folder + 'gs1.jl') as _:
            self._gs1_data = _.read().splitlines()
        self.dataframe_gs1 = self._generate_dataframe(self._gs1_data)

    def _extract_cnpj_data(self) -> None:
        with open(self._folder + 'cnpjs_receita_federal.jl') as _:
            self._cnpj_data = _.read().splitlines()
        self.dataframe_cnpj = self._generate_dataframe(self._cnpj_data)

    def _extract_cosmos_data(self) -> None:
        with open(self._folder + 'cosmos.jl') as _:
            self._cosmos_data = _.read().splitlines()
        self.dataframe_cosmos = self._generate_dataframe(self._cosmos_data)


# To test the file
if __name__ == '__main__':
    ex = Extract('../../data/input')
    ex.extract_bases()
