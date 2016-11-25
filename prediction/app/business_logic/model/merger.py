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

    def __init__(self, name, how, drop_missing, left_keys, right_keys):
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
        self.filter_columns = []

    def set_datastore(self, datastore):
        """define the related datastore to work with

        Args:
            datastore (TYPE): Datastore

        Returns:
            TYPE: Datastore
        """
        self.datastore = datastore

    def merge_and_clean(self):
        self._merge()
        print "self._merge()", self.merged
        self._display_missing_data()
        print "self._display_missing_data()", self.merged
        self._filter_on_columns()
        print "self._filter_on_columns()", self.merged
        return self.merged

    def _merge(self):
        """Summary

        Returns:
            TYPE: Description

        Raises:
            "Abstract: Description
        """
        raise "Abstract Class, method not implemented"

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

    def _filter_on_columns(self):
        self.merged = self.merged[self.filter_columns]
