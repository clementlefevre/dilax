import os
import app.business_logic.model.merger.abstract as abstract
from app.business_logic.service.geocoding_API_service import get_region
from app.business_logic.helper.file_helper import get_file_path
from app.business_logic.model.config_manager import Config_manager


config_manager = Config_manager()

fileDir = os.path.dirname(os.path.abspath(__file__))


class RegionsMerger(abstract.Merger):

    def __init__(self):
        super(RegionsMerger, self).__init__(name="regions",
                                            left_keys=['idbldsite'], right_keys=['idbldsite'], suffixes=['', ''])
        self.filter_columns = None

    def _set_right_data(self):
        df_sites = self.datastore.db_manager.sites
        df_sites['region'] = df_sites.apply(
            lambda row: get_region(row['latitude'], row['longitude']), axis=1)

        df_region_id = abstract.pd.read_csv(get_file_path(
            '../data/regions_countries.csv', fileDir))

        df_sites = abstract.pd.merge(df_sites, df_region_id, on='region', how='left',
                                     indicator=True)

        df_sites = df_sites[['idbldsite', 'sname', 'latitude', 'longitude',
                             'region', 'region_id']]
        self.datastore.sites_infos = df_sites

        self.right = df_sites[['idbldsite', 'region', 'region_id']]

    def _custom(self):
        self.merged = self.merged.drop(['region'], 1)
