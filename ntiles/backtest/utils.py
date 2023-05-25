import pandas as pd
import numpy as np
import numba as nb
import duckdb
from typing import Optional, List, Union


def subset_frame(frame: pd.DataFrame, columns: Optional[List[str]]):
    """
    subsets the given data frame by the given columns
    if the columns are none then the whole frame is returned
    :param frame: the dataframe to subset
    :param columns: the columns we are going to subset by, if none then nothing is done
    :return: Given frame subset by the given columns
    """
    if columns:
        return frame[columns]
    return frame


def get_ntile_cols(frame: pd.DataFrame) -> List[str]:
    """
    :param frame: data frame to get columns of
    :return: all columns in the frame that contain 'Ntile'
    """
    return [col for col in frame.columns if 'Ntile' in col]


def get_non_ntile_cols(frame: pd.DataFrame) -> List[str]:
    """
    :param frame: data frame to get columns of
    :return: all columns in the frame that dont contain 'Ntile'
    """
    return [col for col in frame.columns if 'Ntile' not in col]


def make_nan_inf_summary(df: Union[pd.DataFrame, pd.Series], max_loss: float, print_good: bool = True) -> pd.DataFrame:
    """
    makes a summary fot the the amount of nan and infinity values in the given data frame
    will throw a ValueError if the percent of nan and inf is greater than the given threshold
    prints a summary of the nan's and inf of there are any
    :param df: the data frame we are checking
    :param max_loss: max decimal percent of nan and inf we are allowing the df to contain
    :param print_good: should we print the output if we dropped less then the threshold?
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
        if print_good:
            print(outString)

        # dropping the nans and the infinity values
        df = df.replace([np.inf, -np.inf], np.nan).dropna()

    elif print_good:
        print('Dropped 0% of data')

    return df


def rolling_sum(a, n):
    """
    rolling sum, column wise
    :param a: array to roll and sum
    :param n: length of rolling window
    :return: a[n:, :] of rolling sum
    """
    if n == 1:
        return a

    cum_sum = np.cumsum(a, axis=0)
    cum_sum[n:, :] = cum_sum[n:, :] - cum_sum[:-n, :]
    return cum_sum[n - 1:, :]


@nb.njit(parallel=True)
def correlation_2d(factor: np.array, returns: np.array) -> np.array:
    """
    calculates a timeseries of correlation for the given factor and forward returns
    factor and returns must have EXACTLY the same structure and order of assets/days
    think of each row as a group and we calculate the correlation by groups

    :param factor: 2d np.array, each row represents factor values for different assets on same day
    :param returns: 2d np.array, each row represents forward returns for different assets on same day
    :return:1d np.array representing time series of factor values
    """
    if factor.shape != returns.shape:
        raise ValueError('Factor and returns dont represent same information')

    num_rows = factor.shape[0]
    out = np.empty(shape=num_rows)

    for i in nb.prange(num_rows):
        finite_mask = np.isfinite(factor[i]) & np.isfinite(returns[i])
        out[i] = np.corrcoef(factor[i][finite_mask], returns[i][finite_mask])[0][1]

    return out


def pad_extra_day(matrix_df: pd.DataFrame, pad_value: any) -> pd.DataFrame:
    """
    pads a unstacked frame with a single extra row are the start of the data frame
    :param matrix_df: df to pad, index: pd.Period, columns: any, values: any
    :param pad_value: constant value to insert into a row
    :return: matrix_df with a padded value
    """
    out = matrix_df.copy()
    new_period = (out.index.min().to_timestamp() - pd.DateOffset(1)).to_period('D')
    out.loc[new_period, :] = np.full(shape=out.shape[1], fill_value=pad_value)
    return out.sort_index()  # can make this function better without a sort


def remove_cat_index(frame: Union[pd.Series, pd.DataFrame]) -> Union[pd.Series, pd.DataFrame]:
    """
    if the frame has a categorical index it will remove it
    :return: frame with the categorical index removed
    """
    if frame.index.is_categorical():
        frame.index = frame.index.astype(str)

    return frame


def convert_date_to_period(frame: Union[pd.DataFrame, pd.Series], freq: str = 'D', **kwargs) -> Union[
    pd.DataFrame, pd.Series]:
    """
    converts the date column to a period if the date column is of type timestamp
    if the 'date' column is a period then nothing will be changed
    date can be in the index or columns

    :param frame: the frame containing the date column
    :param freq: the freq for the period
    :return: thr same frame that was passed but 'date' is a partiod.
    """
    index_names = list(frame.index.names)
    frame = frame.reset_index()

    if 'date' in frame.columns:
        frame['date'] = frame['date'].dt.to_period(freq)
        frame.set_index(index_names)
        return frame

    raise ValueError('"date" not found in data frame')


def ntile(factor: pd.Series, ntiles: int, ) -> pd.Series:
    """
    Universe relative Quantiles of a factor by day
    Around 100X faster than pandas groupby qcut

    pd.DataFrame of ntiled factor
        index: (pd.Period, _asset_id)
        Columns: (factor, ntile)
        Values: (factor value, Ntile corresponding to factor value)

    :param factor: same var as ntile_return_tearsheet
    :param ntiles: same var as ntile_return_tearsheet
    """
    factor = factor.to_frame('factor').reset_index()
    index_names = factor.columns.tolist()
    index_names.remove('factor')

    date_is_period = isinstance(factor.date.dtype, pd.core.dtypes.dtypes.PeriodDtype)
    if date_is_period:
        factor['date'] = factor['date'].dt.to_timestamp()

    sql_quantile = f"""SELECT *, NTILE({ntiles}) OVER(PARTITION BY date ORDER BY factor.factor DESC) as ntile
                            FROM factor
                            WHERE factor.factor IS NOT NULL"""
    con = duckdb.connect(':memory:')
    factor_ntile = con.execute(sql_quantile).df()
    con.close()

    if date_is_period:
        factor_ntile['date'] = factor_ntile['date'].dt.to_period(freq='D')

    factor_ntile = factor_ntile.set_index(index_names)
    return factor_ntile
