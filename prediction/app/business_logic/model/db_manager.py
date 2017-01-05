import os
from sqlalchemy import create_engine
from ..model import pd, logging
from ..helper.file_helper import get_file_path

fileDir = os.path.dirname(os.path.abspath(__file__))


class DB_manager(object):

    def __init__(self, db_params):
        self.params = db_params
        self._create_connection()
        self._init_data()

    def _db_address(self):

        credentials = self.params['db_user'] + ':' + self.params['db_pwd']
        address = 'postgresql://' + credentials + '@' + \
            self.params['db_url'] + ':' + \
            self.params['db_port'] + '/' + self.params['db_name']
        return address

    def _create_connection(self):
        try:
            self.engine = create_engine(self._db_address())
            logging.info("engine created : " + str(self.engine))
        except Exception as e:
            logging.error(
                "Could not create connection for {}".format(self.db_params))
            logging.error(e.message)

    def _query(self, table):
        try:
            result = pd.read_sql_query("select * from " + '"' + table + '"',
                                       con=self.engine)
        except Exception as e:
            logging.error("Could not query db for {}".format(table))
            logging.error(e.message)
        return result

    def _init_data(self):
        self.sites = self._query("dwe_bld_site")
        self.counts = self._query("cnt_aggregated_totalin_site_day")
        self.addresses = self._query("dwe_bld_address")
        self.weather_day = self._query("dwe_ext_weather_meteogroup_day")
        self.weather_intraday = self._query(
            "dwe_ext_weather_meteogroup_intraday")
        self.public_holidays = self._query("dwe_cal_holiday")
        self.conversion = self._query("dwe_ext_conversion")
        

    def has_table(self, table_name):

        return self.engine.dialect.has_table(self.engine, table_name)
