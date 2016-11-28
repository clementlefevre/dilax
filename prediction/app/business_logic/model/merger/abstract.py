"""Abstract Class for the table merging process
"""
import logging
import pandas as pd


class Merger(object):
    """Summary

    Attributes:
        datastore (TYPE): Description
        drop_missing (TYPE): Description
        how (TYPE): Description
        left_keys (TYPE): Description
        name (TYPE): Description
        right_keys (TYPE): Description
    """

    def __init__(self, name=None, how='left', drop_missing=True, left_keys=None, right_keys=None, suffixes=None, indicator=True):
        """Summary

        Args:
            name (TYPE): Description
            how (TYPE): Description
            drop_missing (TYPE): Description
            left_keys (TYPE): Description
            right_keys (TYPE): Description
        """
        self.name = name
        self.how = how
        self.left_keys = left_keys
        self.right_keys = right_keys
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
        self._merge()
        self._display_missing_data()
        self._custom()
        self._filter_on_columns()
        self._rename_columns()
        self._drop_columns()

    def _merge(self):

        self.merged = pd.merge(self.left, self.right,
                               left_on=self.left_keys, right_on=self.right_keys,
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
            self.merged = self.merged[self.filter_columns]

    def _rename_columns(self):
        self.merged.rename(columns=self.rename_columns, inplace=True)

    def _drop_columns(self):
        for col in self.drop_columns:
            self.merged.drop(col, 1)
