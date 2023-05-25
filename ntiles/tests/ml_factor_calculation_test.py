import unittest
from abc import ABC

import numpy as np
import pandas as pd

from toolbox.utils.ml_factor_calculation import ModelWrapper, calc_ml_factor, generate_indexes
from toolbox.utils.ml_factor_calculation import SliceHolder


class MyTestCase(unittest.TestCase):

    def examples(self):
        # index includes non trading days
        # exactly 60 occurrences of each ticker
        first = pd.Timestamp(year=2010, month=1, day=1)
        self.date_index = pd.MultiIndex.from_product(
            [pd.date_range(start=first, end=pd.Timestamp(year=2010, month=3, day=1)),
             ['BOB', 'JEFF', 'CARL']], names=['date', 'symbol'])

        self.expected_index_e5_10_30 = [
            (SliceHolder(first, first + pd.Timedelta(days=29)),
             SliceHolder(first + pd.Timedelta(days=40), first + pd.Timedelta(days=44))),

            (SliceHolder(first, first + pd.Timedelta(days=34)),
             SliceHolder(first + pd.Timedelta(days=45), first + pd.Timedelta(days=49))),

            (SliceHolder(first, first + pd.Timedelta(days=39)),
             SliceHolder(first + pd.Timedelta(days=50), first + pd.Timedelta(days=54))),

            (SliceHolder(first, first + pd.Timedelta(days=44)),
             SliceHolder(first + pd.Timedelta(days=55), first + pd.Timedelta(days=59)))
        ]

        self.expected_index_e7_8_30 = [
            (SliceHolder(first, first + pd.Timedelta(days=29)),
             SliceHolder(first + pd.Timedelta(days=37), first + pd.Timedelta(days=44))),

            (SliceHolder(first, first + pd.Timedelta(days=37)),
             SliceHolder(first + pd.Timedelta(days=45), first + pd.Timedelta(days=52))),

            (SliceHolder(first, first + pd.Timedelta(days=45)),
             SliceHolder(first + pd.Timedelta(days=53), first + pd.Timedelta(days=59))),
        ]

        self.expected_index_e5_10_30 = self.turn_to_datetime64(self.expected_index_e5_10_30)
        self.expected_index_e7_8_30 = self.turn_to_datetime64(self.expected_index_e7_8_30)

        self.expected_index_r5_10_30 = [
            (SliceHolder(first, first + pd.Timedelta(days=29)),
             SliceHolder(first + pd.Timedelta(days=40), first + pd.Timedelta(days=44))),

            (SliceHolder(first + pd.Timedelta(days=5), first + pd.Timedelta(days=34)),
             SliceHolder(first + pd.Timedelta(days=45), first + pd.Timedelta(days=49))),

            (SliceHolder(first + pd.Timedelta(days=10), first + pd.Timedelta(days=39)),
             SliceHolder(first + pd.Timedelta(days=50), first + pd.Timedelta(days=54))),

            (SliceHolder(first + pd.Timedelta(days=15), first + pd.Timedelta(days=44)),
             SliceHolder(first + pd.Timedelta(days=55), first + pd.Timedelta(days=59)))
        ]

        self.expected_index_r7_8_30 = [
            (SliceHolder(first, first + pd.Timedelta(days=29)),
             SliceHolder(first + pd.Timedelta(days=37), first + pd.Timedelta(days=44))),

            (SliceHolder(first + pd.Timedelta(days=8), first + pd.Timedelta(days=37)),
             SliceHolder(first + pd.Timedelta(days=45), first + pd.Timedelta(days=52))),

            (SliceHolder(first + pd.Timedelta(days=16), first + pd.Timedelta(days=45)),
             SliceHolder(first + pd.Timedelta(days=53), first + pd.Timedelta(days=59))),
        ]

        self.expected_index_r5_10_30 = self.turn_to_datetime64(self.expected_index_r5_10_30)
        self.expected_index_r7_8_30 = self.turn_to_datetime64(self.expected_index_r7_8_30)

        class FooModel(ModelWrapper, ABC):
            def fit_model(self, tf: pd.DataFrame, tt: pd.Series):
                pass

        self.fooModel = FooModel()

        self.foo_target = pd.Series(index=self.date_index, dtype='float64')
        self.foo_target.loc[:] = 0
        self.fooFeatures = pd.DataFrame(index=self.date_index)
        self.fooFeatures.loc[:] = 0

    #
    #  ************************************  generate_indexes  ************************************
    #

    def test_expanding_generateIndexes(self):
        """
        testing generate indexes using the expanding param
        Turning slice lists to string. Comparing equality of np.datetime64 is annoying
        """
        self.examples()

        # no left over days all even slices
        returnedIndexesE10_5_30 = list(
            generate_indexes(data_index=self.date_index, eval_days=10, refit_every=5, expanding=30))
        self.assertEqual(str(self.expected_index_e5_10_30), str(returnedIndexesE10_5_30))

        # left over days last slice will be of size 1
        returnedIndexesE7_8_30 = list(
            generate_indexes(data_index=self.date_index, eval_days=7, refit_every=8, expanding=30))
        self.assertEqual(str(self.expected_index_e7_8_30), str(returnedIndexesE7_8_30))

    def test_rolling_generateIndexes(self):
        """
        testing generate indexes using the rolling param
        Turning slice lists to string. Comparing equality of np.datetime64 is annoying
        """
        self.examples()
        # no left over days all even slices
        returnedIndexesR10_5_30 = list(
            generate_indexes(data_index=self.date_index, eval_days=10, refit_every=5, rolling=30))
        self.assertEqual(str(self.expected_index_r5_10_30), str(returnedIndexesR10_5_30))

        # left over days last slice will be of size 1
        returnedIndexesR7_8_30 = list(
            generate_indexes(data_index=self.date_index, eval_days=7, refit_every=8, rolling=30))

        self.assertEqual(str(self.expected_index_r7_8_30), str(returnedIndexesR7_8_30))

    #
    #  ************************************  calcMlFactor  ************************************
    #

    def test_negative_calcMlFactor(self):
        """
        testing for error when eval_days, refit_every, expanding, rolling  is less than one
        this also tests generate_indexes
        """
        self.examples()

        # eval_days
        with self.assertRaises(ValueError) as em:
            calc_ml_factor(model=self.fooModel, features=self.fooFeatures, target=self.foo_target, eval_days=0,
                           refit_every=1, expanding=1)
        self.assertEqual('eval_days and/or refit_every must be greater than zero', str(em.exception))

        # refit_every
        with self.assertRaises(ValueError) as em:
            calc_ml_factor(model=self.fooModel, features=self.fooFeatures, target=self.foo_target, eval_days=1,
                           refit_every=0, expanding=1)
        self.assertEqual('eval_days and/or refit_every must be greater than zero', str(em.exception))

        # expanding
        with self.assertRaises(ValueError) as em:
            calc_ml_factor(model=self.fooModel, features=self.fooFeatures, target=self.foo_target, eval_days=1,
                           refit_every=1, expanding=0)
        self.assertEqual('expanding must be greater than zero', str(em.exception))

        # rolling
        with self.assertRaises(ValueError) as em:
            calc_ml_factor(model=self.fooModel, features=self.fooFeatures, target=self.foo_target, eval_days=1,
                           refit_every=1, rolling=0)
        self.assertEqual('rolling must be greater than zero', str(em.exception))

    def test_rollingAndExpanding_calcMlFactor(self):
        """
        testing for error when rolling days and expanding are both defined and not defined
        """
        self.examples()

        with self.assertRaises(ValueError) as em:
            calc_ml_factor(model=self.fooModel, features=self.fooFeatures, target=self.foo_target, eval_days=1,
                           refit_every=1, rolling=1, expanding=1)
        self.assertEqual('minTrainDays and rollingDays can not both be defined', str(em.exception))

        with self.assertRaises(ValueError) as em:
            calc_ml_factor(model=self.fooModel, features=self.fooFeatures, target=self.foo_target, eval_days=1,
                           refit_every=1)
        self.assertEqual('minTrainDays or rollingDays must be defined', str(em.exception))

    def test_contain_bad_val_calc_ml_factor(self):
        """
        testing for when the given features and target have nan values
        """
        self.examples()
        # features has a nan
        with self.assertRaises(ValueError) as em:
            self.fooFeatures[0] = 0.0
            self.fooFeatures.iat[1, 0] = np.nan
            calc_ml_factor(model=self.fooModel, features=self.fooFeatures, target=self.foo_target, eval_days=1,
                           refit_every=1)
        self.assertEqual('There are nan or inf values in the features', str(em.exception))

        # features has a inf
        self.examples()
        with self.assertRaises(ValueError) as em:
            self.fooFeatures[0] = 0.0
            self.fooFeatures.iat[1, 0] = np.inf
            calc_ml_factor(model=self.fooModel, features=self.fooFeatures, target=self.foo_target, eval_days=1,
                           refit_every=1)
        self.assertEqual('There are nan or inf values in the features', str(em.exception))

        # target has a nan
        self.examples()
        with self.assertRaises(ValueError) as em:
            self.foo_target.iat[1] = np.nan
            calc_ml_factor(model=self.fooModel, features=self.fooFeatures, target=self.foo_target, eval_days=1,
                           refit_every=1)
        self.assertEqual('There are nan or inf values in the target', str(em.exception))

        # target has a inf
        self.examples()
        with self.assertRaises(ValueError) as em:
            self.foo_target.iat[1] = np.inf
            calc_ml_factor(model=self.fooModel, features=self.fooFeatures, target=self.foo_target, eval_days=1,
                           refit_every=1)
        self.assertEqual('There are nan or inf values in the target', str(em.exception))

    @staticmethod
    def turn_to_datetime64(convert):
        """
        helper converts SliceHolder of pd.Timestamp to SliceHolder of np.datetime64
        """
        return [(SliceHolder(s[0].start.to_datetime64(), s[0].end.to_datetime64()),
                 SliceHolder(s[1].start.to_datetime64(), s[1].end.to_datetime64()))
                for s in convert]


if __name__ == '__main__':
    unittest.main()
