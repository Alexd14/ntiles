import unittest

from pandas import (
    Timestamp,
    DataFrame,
    concat,
    MultiIndex
)

from ntiles.toolbox.constitutes.constitute_adjustment import ConstituteAdjustment
from ntiles.toolbox.utils.date_config import DateConfig


class ConstituteAdjustmentTest(unittest.TestCase):

    def examples(self):
        self.foo_constitutes = DataFrame(data=[
            # symbol    entered     exited
            ['BOB', '2009-01-01', '2012-01-01'],  # whole thing
            ['LARY', '2010-01-05', '2010-01-07'],  # added and then exited
            ['JEFF', '2011-03-02', '2020-03-02']],  # added too late
            columns=['symbol', 'from', 'thru']
        )
        self.date_config = DateConfig(freq='D', date_format='%Y-%m-%d', target_data_type='timestamp')
        self.ca = ConstituteAdjustment(id_col='symbol', date_config=self.date_config)
        self.ca.add_universe_info(universe=self.foo_constitutes, start_date='2010-01-04', end_date='2010-01-12', )

        self.foo_data = DataFrame(
            data=[['BOB', '2010-01-04', 50],
                  ['BOB', '2010-01-05', 51],
                  ['BOB', '2010-01-06', 52],
                  ['BOB', '2010-01-07', 53],
                  # ['BOB', '2010-01-08', 54], this will be missing data
                  ['BOB', '2010-01-11', 55],
                  ['BOB', '2010-01-12', 56],
                  ['LARY', '2010-01-04', 20],  # should not be included
                  ['LARY', '2010-01-05', 21],
                  ['LARY', '2010-01-06', 22],
                  ['LARY', '2010-01-07', 23],
                  ['LARY', '2010-01-08', 24],  # should not be included
                  ['LARY', '2010-01-11', 25],  # should not be included
                  ['LARY', '2010-01-12', 26],  # should not be included
                  ['LARY', '2010-01-13', 27],  # should not be included
                  ['FOO', '2010-01-08', 0]],  # should be ignored
            columns=['symbol', 'date', 'factor'])

        self.adjusted_foo = DataFrame(
            data=[['BOB', Timestamp('2010-01-04'), 50],
                  ['BOB', Timestamp('2010-01-05'), 51],
                  ['BOB', Timestamp('2010-01-06'), 52],
                  ['BOB', Timestamp('2010-01-07'), 53],
                  ['BOB', Timestamp('2010-01-08'), None],
                  ['BOB', Timestamp('2010-01-11'), 55],
                  ['BOB', Timestamp('2010-01-12'), 56],
                  ['LARY', Timestamp('2010-01-05'), 21],
                  ['LARY', Timestamp('2010-01-06'), 22],
                  ['LARY', Timestamp('2010-01-07'), 23]],
            columns=['symbol', 'date', 'factor']).set_index(['date', 'symbol'])

        pricing_data = DataFrame(
            data=[['LARY', Timestamp('2010-01-08'), 24],
                  ['LARY', Timestamp('2010-01-11'), 25],
                  ['LARY', Timestamp('2010-01-12'), 26]],
            columns=['symbol', 'date', 'factor']).set_index(['date', 'symbol'])

        self.adjusted_pricing = concat([pricing_data, self.adjusted_foo]).sort_values(['symbol', 'date'])

    #
    #  ************************************  add_universe_info  ************************************
    #

    def test_factor_add_universe_info(self):
        """
        testing the index generation in add_universe_info
        has missing data (None), data that should not be included (yet to be added, has been removed) and
        irrelevant symbols
        """
        self.examples()

        # for factors
        factor_components = [(Timestamp('2010-01-04'), 'BOB'),
                             (Timestamp('2010-01-05'), 'BOB'),
                             (Timestamp('2010-01-06'), 'BOB'),
                             (Timestamp('2010-01-07'), 'BOB'),
                             (Timestamp('2010-01-08'), 'BOB'),
                             (Timestamp('2010-01-11'), 'BOB'),
                             (Timestamp('2010-01-12'), 'BOB'),
                             (Timestamp('2010-01-05'), 'LARY'),
                             (Timestamp('2010-01-06'), 'LARY'),
                             (Timestamp('2010-01-07'), 'LARY')]

        self.assertTrue(MultiIndex.from_tuples(factor_components).equals(self.ca.factor_components))

    def test_throw_column_error(self):
        """
        ensuring a error will be thrown when the correct columns are not supplied
        """
        self.examples()

        with self.assertRaises(ValueError) as em:
            self.ca.add_universe_info(start_date='2010-01-04',
                                      end_date='2010-01-12',
                                      universe=DataFrame(columns=['foo', 'foo1', 'foo2']))
        self.assertEqual('Required column "symbol" is not present', str(em.exception))

    def test_duplicate_symbols(self):
        """
        Ensuring that passing a df with duplicate symbols will raise a ValueError
        """
        self.examples()

        self.foo_constitutes.iat[1, 0] = 'BOB'

        with self.assertRaises(ValueError) as em:
            self.ca.add_universe_info(start_date='2010-01-04',
                                      end_date='2010-01-12',
                                      universe=self.foo_constitutes)
        self.assertEqual('The column symbol is 0.333 duplicates, 1 rows\n', str(em.exception))

    #
    #  ************************************  adjust_data_for_membership  ************************************
    #

    def test_adjust_data_for_membership(self):
        """
        ensuring adjust_data_for_membership return the correct data frame
        data given has good data to index, not seen bad tickers, and tickers with dates out of bounds
        """
        self.examples()
        filtered = self.ca.adjust_data_for_membership(data=self.foo_data)
        self.assertTrue(self.adjusted_foo['factor'].sort_index().equals(filtered.sort_index()))

    def test_throw_error_adjust_data_for_membership(self):
        """
        ensuring adjust_data_for_membership throws error when not given symbols or date
        """
        self.examples()

        with self.assertRaises(ValueError) as em:
            self.ca.adjust_data_for_membership(data=DataFrame(columns=['foo', 'notSymbol', 'factor']))
        self.assertEqual('Required column "date" is not present', str(em.exception))

    def test_no_index_set_adjust_data_for_membership(self):
        """
        ensuring adjust_data_for_membership throws error when there is no index set
        AKA add_universe_info was never called
        """
        self.examples()

        with self.assertRaises(ValueError) as em:
            ConstituteAdjustment().adjust_data_for_membership(data=self.foo_data)
        self.assertEqual('Universe is not set', str(em.exception))


if __name__ == '__main__':
    unittest.main()
