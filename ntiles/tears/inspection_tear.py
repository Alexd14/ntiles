from abc import ABC

from ntiles.tears.base_tear import BaseTear
from ntiles import plotter


class InspectionTear(BaseTear, ABC):
    """
    creates a data inspection sheet
    """

    def __init__(self, factor_data):
        """
        :param factor_data: factor_data from ntiles
        """
        super().__init__()
        self._factor_data = factor_data

    def compute(self) -> None:
        """
        kicks off the tearsheet
        :return: None
        """
        # summary sheet: mean, median, standard deviation, % of total values in bin,
        # data points over time all
        # data point over time ntile
        # mean value of factor per ntile over time
        self.make_summary_frame()
        self.timeseries_plots()

    def make_summary_frame(self):
        quantile_stats = self._factor_data.groupby('ntile').agg(['median', 'std', 'min', 'max', 'count']).factor
        quantile_stats['count %'] = quantile_stats['count'] / quantile_stats['count'].sum() * 100

        # aesthetics
        quantile_stats = quantile_stats.round(2)
        quantile_stats.columns = [col.title() for col in quantile_stats.columns]
        quantile_stats.index.name = 'Ntile:'

        plotter.render_table(quantile_stats, 'Quantiles Statistics')

    def timeseries_plots(self):
        no_index_factor_data = self._factor_data.reset_index().dropna()
        date_agg = no_index_factor_data.groupby('date')
        date_ntile_agg = no_index_factor_data.groupby(['date', 'ntile'])

        plotter.plot_inspection_data(date_agg.factor.count(), 'Universe Count Of Factor Per Day', 'Count')
        plotter.plot_inspection_data(date_ntile_agg.factor.count().unstack(), 'Ntile Count of Factor Per Day', 'Count')
        plotter.plot_inspection_data(date_ntile_agg.factor.median().unstack(), 'Median Factor Value by Ntile', 'Median',
                                     2)
