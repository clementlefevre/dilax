"""Abstract Class for the table merging process
"""
import os
import logging
import pandas as pd
from app.business_logic.helper.file_helper import get_file_path

from app.business_logic.model.config_manager import Config_manager


config_manager = Config_manager()

fileDir = os.path.dirname(os.path.abspath(__file__))


class Merger(object):

    def __init__(self, name=None, how='left', drop_missing=True, left_on=None, right_on=None, suffixes=None, indicator=True):

        self.name = name
        self.how = how
        self.left_on = left_on
        self.right_on = right_on
        self.drop_missing = drop_missing
        self.suffixes = suffixes
        self.indicator = indicator
        self.filter_columns = []
        self.rename_columns = {}
        self.drop_columns = []

    def set_datastore(self, datastore):
        self.datastore = datastore

    def _set_left_data(self, data):
        self.left = data

    def _set_right_data(self):
        raise(NotImplementedError)

    def merge_and_clean(self, left_data):
        self._set_left_data(left_data)
        self._set_right_data()
        if self.right is None:
            self.merged = left_data
            return
        self._merge()
        self._display_missing_data()
        self._custom()
        self._filter_on_columns()
        self._rename_columns()
        self._drop_columns()

    def _merge(self):
        self.merged = pd.merge(self.left, self.right,
                               left_on=self.left_on, right_on=self.right_on,
                               suffixes=self.suffixes,
                               indicator=self.indicator, how=self.how)

    def _display_missing_data(self):

        df_missing = self.merged[self.merged._merge == "left_only"]

        if df_missing.empty:
            logging.info("No missing data after {}.".format(self.name))
        else:
            sites_missing = df_missing.groupby('idbldsite')._merge.count()

            logging.warning("after {} : No data found for :".format(self.name))
            logging.warning(sites_missing)

        if self.drop_missing:
            self.merged = self.merged[self.merged._merge == "both"]

        self.merged = self.merged.drop("_merge", 1)

    def _custom(self):
        pass

    def _filter_on_columns(self):
        if self.filter_columns is not None:
            try:
                self.merged = self.merged[self.filter_columns]
            except KeyError as e:
                logging.error(
                    "Could not filter on columns : {}".format(e.message))

    def _rename_columns(self):
        self.merged.rename(columns=self.rename_columns, inplace=True)

    def _drop_columns(self):
        for col in self.drop_columns:
            try:
                self.merged = self.merged.drop(col, 1)
                logging.info("This columns has been removed :{}".format(col))
            except (KeyError, ValueError) as e:
                logging.warning(
                    "This column does not exist in the dataframe and thus cannot be deleted :{}".format(col))
