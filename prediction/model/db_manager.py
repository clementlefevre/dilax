from sqlalchemy import create_engine
from model import pd
from config import *


class DB_manager(object):

    def __init__(self, db_params={}):
        self.params = db_params
        self.__create_connection()
        self.__init_data()

    def __db_address(self):

        credentials = self.params['db_user'] + ':' + self.params['db_pwd']
        address = 'postgresql://' + credentials + '@' + \
            self.params['db_url'] + ':' + \
            self.params['db_port'] + '/' + self.params['db_name']
        return address

    def __create_connection(self):
        self.engine = create_engine(self.__db_address())
        print "engine created" + str(self.engine)

    def __query(self, table):
        return pd.read_sql_query("select * from " + '"' + table + '"',
                                 con=self.engine)

    def __init_data(self):
        self.sites = self.__query("dwe_bld_site")
        self.counts = self.__query("dwe_cnt_site")
        self.addresses = self.__query("dwe_bld_address")
        self.weather_day = self.__query("dwe_ext_weather_meteogroup_day")
        self.weather_intraday = self.__query(
            "dwe_ext_weather_meteogroup_intraday")
        self.public_holidays = self.__query("dwe_cal_holiday")
