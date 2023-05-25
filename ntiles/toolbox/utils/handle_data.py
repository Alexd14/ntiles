from typing import List

import numpy as np
import pandas as pd


def handle_duplicates(df: pd.DataFrame, out_type: str, name: str, drop: bool = False,
                      subset: List[any] = None) -> pd.DataFrame:
    """
    Checking to see if there are duplicates in the given data frame
    if there are duplicates outType will be used
        Ex: give a Warning or raise ValueError
    :param df: The data we are checking
    :param name: the name of the data to give as output
    :param out_type: what to do do if there are duplicates. Currently supports "Warning", "ValueError"
    :param drop: boolean to drop the duplicates or not
        if False no data frame will be returned and vice verse
        this param will not matter if outType is a ValueError
    :param subset: subset of df columns we should check duplicates for
    :return: the given df with duplicates dropped according to drop
    """
    # seeing if there are duplicates in the factor
    dups = df.duplicated(subset=subset)

    if dups.any():
        amount_of_dups = dups.sum()
        out_string = f'{name} is {round(amount_of_dups / len(df), 3)} duplicates, {amount_of_dups} rows\n'
        if out_type == 'Warning':
            Warning(out_string)
        elif out_type == 'ValueError':
            raise ValueError(out_string)
        else:
            raise ValueError(f'out_type {out_type} not recognised')

        # dropping the duplicates
        if drop:
            return df.drop_duplicates(subset=subset, keep='first')

    if drop:
        return df


def make_nan_inf_summary(df: pd.DataFrame, max_loss: float) -> pd.DataFrame:
    """
    makes a summary fot the the amount of nan and infinity values in the given data frame
    will throw a ValueError if the percent of nan and inf is greater than the given threshold
    prints a summary of the nan's and inf of there are any
    :param df: the data frame we are checking
    :param max_loss: max decimal percent of nan and inf we are allowing the df to contain
    :return: pandas data frame with the nan and inf dropped
    """
    df_numpy = df.to_numpy()
    nan_array = np.isnan(df_numpy)
    finite_array = np.logical_or(np.isinf(df_numpy), np.isneginf(df_numpy))

    if nan_array.any() or (not finite_array.all()):
        factor_length = len(df)
        amount_nan = nan_array.sum()
        amount_inf = finite_array.sum()
        total_percent_dropped = (amount_nan + amount_inf) / factor_length

        outString = f'Dropped {round(total_percent_dropped * 100, 2)}% of data. ' \
                    f'{round((amount_nan / factor_length) * 100, 2)}% due to nan, ' \
                    f'{round((amount_inf / factor_length) * 100, 2)}% of inf values. Threshold: {max_loss * 100}%\n'

        if total_percent_dropped > max_loss:
            raise ValueError('Exceeded Nan Infinity Threshold. ' + outString)

        # print out string as a summary
        print(outString)

        # dropping the nans and the infinity values
        df = df.replace([np.inf, -np.inf], np.nan).dropna()

    else:
        print('Dropped 0% of data')

    return df
