import pandas as pd

pd.options.display.max_columns = 500


class Transform:
    # Private dataframes
    _dataframe_info_mix = ''
    _dataframe_descriptions = ''
    _dataframe_gs1 = ''
    _dataframe_cnpj = ''
    _dataframe_cosmos = ''

    # Public dataframes
    final_dataframe = ''

    # This class receives the result of the extract dataframes
    def __init__(self
                 , dataframe_info_mix
                 , dataframe_descricoes
                 , dataframe_gs1
                 , dataframe_cnpj
                 , dataframe_cosmos):
        self._dataframe_info_mix = dataframe_info_mix
        self._dataframe_descriptions = dataframe_descricoes
        self._dataframe_gs1 = dataframe_gs1
        self._dataframe_cnpj = dataframe_cnpj
        self._dataframe_cosmos = dataframe_cosmos

    # The principal method (the only one public method), him will call all other methods to run
    def transform_dataframes(self):
        print('Starting of Transform step')
        self._clean_dataframes()

        self._clean_dataframe_descriptions()

        self._valid_gtin()

        self._join_cnpj()

        self._join_description()

        self._clean_final_dataframe()

        print('Finishing Transform step')
        print('---------------------------------')

    # Basic cleaning, taking only the columns we will need, removing duplicates and null values
    def _clean_dataframes(self):
        self._dataframe_info_mix = self._dataframe_info_mix[['gtin', 'category']]

        self._dataframe_gs1 = self._dataframe_gs1[
            ['gtin', 'cnpj_manufacturer', 'response.gepirParty.partyDataLine.address.city']]

        self._dataframe_gs1 = self._dataframe_gs1.dropna()

        self._dataframe_cnpj = self._dataframe_cnpj[['cnpj', 'response.nome', 'response.uf']]

        self._dataframe_cnpj = self._dataframe_cnpj.dropna()

        self._dataframe_cnpj = self._dataframe_cnpj.drop_duplicates()

    # Because description file has a lot of duplicate values, this method is assuming that the description with more
    # letters will be the better description and then will remove all duplicated gtins
    def _clean_dataframe_descriptions(self):
        self._dataframe_descriptions['length'] = self._dataframe_descriptions.description.str.len()

        _ = self._dataframe_descriptions.groupby(['gtin'], sort=False)['length'].max()

        self._dataframe_descriptions = pd.merge(_, self._dataframe_descriptions, on=['gtin', 'length'], how='inner')

        self._dataframe_descriptions = self._dataframe_descriptions[['gtin', 'description']]

    # Merge gtin with info_mix to final_dataframe
    def _valid_gtin(self):
        self.final_dataframe = pd.DataFrame.merge(self._dataframe_gs1, self._dataframe_info_mix, on='gtin', how='inner')

        self.final_dataframe = self.final_dataframe.drop_duplicates()

    # Merge final_dataframe with cnpj to final_dataframe
    def _join_cnpj(self):
        self.final_dataframe = pd.merge(self.final_dataframe, self._dataframe_cnpj, left_on='cnpj_manufacturer',
                                        right_on='cnpj', how='inner')

        self.final_dataframe = self.final_dataframe.drop_duplicates()

    # Merge final_dataframe with description to final_dataframe
    def _join_description(self):
        self.final_dataframe = pd.merge(self.final_dataframe, self._dataframe_descriptions, on='gtin', how='inner')

        self.final_dataframe = self.final_dataframe.drop_duplicates()

    # Renaming final dataframe columns and re-organizing columns
    def _clean_final_dataframe(self):
        self.final_dataframe = self.final_dataframe[
            ['gtin', 'cnpj', 'response.nome', 'response.gepirParty.partyDataLine.address.city', 'response.uf',
             'description', 'category']]

        self.final_dataframe = self.final_dataframe.rename(columns={
            'response.nome': 'razao_social'
            , 'response.gepirParty.partyDataLine.address.city': 'cidade'
            , 'response.uf': 'estado'
        })


# To test this file
if __name__ == '__main__':
    import Extract

    ex = Extract.Extract('../../data/input')
    ex.extract_bases()

    tr = Transform(
        ex.dataframe_info_mix
        , ex.dataframe_descricoes
        , ex.dataframe_gs1
        , ex.dataframe_cnpj
        , ex.dataframe_cosmos
    )

    tr.transform_dataframes()

    print(tr.final_dataframe.head())
