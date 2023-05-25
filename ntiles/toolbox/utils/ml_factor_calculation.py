import gc
from abc import ABC, abstractmethod

import pandas as pd
import numpy as np

from typing import Generator, Tuple, List

from tqdm import tqdm


class ModelWrapper(ABC):
    """
    Wraps a model for calc_ml_factor.
    """
    @abstractmethod
    def fit_model(self, train_features: pd.DataFrame, train_target: pd.Series) -> any:
        """
        Wraps a model for use by the calcMlFactor function.
        Fits a model to the given features. then returns the fit model.
        If the fit model does not contain a "predict" method then predict mut be overwritten.

        :param train_features: the features to train the model on
            Must have the same index as train_target
        :param train_target: the target for the train_features.
            Must have the same index as train_features
        :return: a model fit to the given features and targets
        """
        pass

    @staticmethod
    def transform_data(train_features: pd.DataFrame, train_target: pd.Series, predict: pd.DataFrame) -> \
            Tuple[pd.DataFrame, pd.DataFrame]:
        """
        *** Do not fit any transformations on the predict data. That WILL result in lookahead Bias.***
        Only manipulate the predict data with transformations fit with the train_features

        This method is used to preprocess the data before the training, and predicting data is passed to the model

        The indexes must not be changed. However columns can be dropped and altered.
        Any change to the train_target must also be done to the predict data.

        Example use: fit a PCA to the train_features then transform the train_features and predict data using said PCA.
                or use RFE to reduce dimensionality

        :param train_features: the features to train the model on
        :param train_target: the target for the train_features
        :param predict: The data to make predictions on
        :return: the transformed (train_features, predict) with no index changes.
        """
        return train_features, predict

    def predict(self, train_features: pd.DataFrame, train_target: pd.Series, predict: pd.DataFrame) -> pd.Series:
        """
        fits a model to the given training data and then makes predictions with the fitted model
        fits a model by calling "fitModel".
        assumes the "fitModel" returns a model with a "predict" method.

        :param train_features: the features to train the model on
            Must have the same index as train_target
        :param train_target: the target for the train_features.
            Must have the same index as train_features
        :param predict: The data to make predictions on
        :return: a Tuple of pandas Series with the predictions and a float what s the
        """
        # checks the index but is very slow
        # if not train_features.index.equals(train_target.index):
        #     raise ValueError('The index for the features and target is different')

        # allowing the user to adjust the data before fitting, assuming that the user does not mess up the indexes
        transformed_features, transformedPredict = self.transform_data(train_features, train_target, predict)

        # fitting and making predictions with user defined model
        model: any = self.fit_model(transformed_features, train_target)
        predicted: pd.Series = pd.Series(data=model.predict(transformedPredict), index=predict.index)

        del model, train_features, train_target, predict, transformed_features, transformedPredict
        gc.collect()

        return predicted


class SliceHolder:
    """
    holds information on the start and end indexes for a slice.
    assumes start and end are immutable references
    """

    def __init__(self, start, end):
        self.__start = start
        self.__end = end

    @property
    def start(self):
        return self.__start

    @property
    def end(self):
        return self.__end

    def __str__(self):
        return str(self.__start) + ', ' + str(self.__end)

    def __repr__(self):
        return self.__str__()


def calc_ml_factor(model: ModelWrapper, features: pd.DataFrame, target: pd.Series, eval_days: int, refit_every: int,
                   expanding: int = None, rolling: int = None) -> pd.Series:
    """
    Calculates an alpha factor using a ML factor combination method.
    The model is fit and predictions are made in a ModelWrapper
    This function organizes the data so the model can make unbiased predictions
    on what would have been point in time data.

    this function assumes that the data passed has all trading days in it (first level of index).
    Ex if the the data is missing for one day then we will miss a

    :param model: the ModelWrapper that will be used to make predictions.
    :param features: the features to train the model on
        there cannot be null values
        must have a multi index of (pd.Timestamp, symbol)
    :param target: the target we are going to fit the model to
        there cannot be null values
        must have a multi index of (pd.Timestamp, symbol)
    :param eval_days: IF INCORRECT THERE WILL BE LOOK AHEAD BIAS
        the amount of days it takes to know the predictions outcome
        this number should simply be the length of return we are trying to predict
    :param refit_every: the amount of consecutive days to predict using a single model
        this is essentially saying refit the model every x days
    :param expanding: the minimum amount of days of data to train on
        if rollingDays is passed then this should not be passed
        if this value is passed then the model will be trained with an expanding window of data
    :param rolling: the amount of rolling days to fit a model to
        if minTrainDays is passed then this should not be passed
    :return: pandas series of predictions. The index will be the same as "features"
    """

    features_copy: pd.DataFrame = features.copy().sort_index()
    target_copy: pd.Series = target.copy().sort_index()

    if not np.isfinite(features_copy.values).all():
        raise ValueError('There are nan or inf values in the features')
    if not np.isfinite(target_copy.values).all():
        raise ValueError('There are nan or inf values in the target')
    if not isinstance(features_copy.index, pd.MultiIndex):
        raise ValueError('Features and target must have a pd.MultiIndex of (pd.Timestamp, str)')
    if not isinstance(features_copy.index.get_level_values(0), pd.DatetimeIndex):
        raise ValueError('Features and target must have index level 0 of pd.DatetimeIndex')
    if not features_copy.index.equals(target_copy.index):
        raise ValueError('The index for the features and target is different')

    train_predict_slices: Generator[Tuple[SliceHolder, SliceHolder], None, None] = \
        generate_indexes(features_copy.index, eval_days, refit_every, expanding, rolling)

    ml_alpha: List[pd.Series] = []
    for train_slice, predict_slice in tqdm(train_predict_slices):
        features_train = features_copy.loc[train_slice.start:train_slice.end]
        target_train = target_copy.loc[train_slice.start:train_slice.end]
        predict = features_copy.loc[predict_slice.start:predict_slice.end]
        ml_alpha.append(model.predict(features_train, target_train, predict))

    del features_copy, target_copy
    gc.collect()

    return pd.concat(ml_alpha)


def generate_indexes(data_index: pd.MultiIndex, eval_days: int, refit_every: int, expanding: int = None,
                     rolling: int = None) -> Generator[Tuple[SliceHolder, SliceHolder], None, None]:
    """
    generates the slice index's for the training and predicting periods.
    function is designed to work with dates in level 0 however this is not enforced anywhere

    :param data_index: MultiIndex of the data we are generating int index's for
    :param eval_days: IF INCORRECT THERE WILL BE LOOK AHEAD BIAS
        the amount of days it takes to know the predictions outcome
        this number should simply be the length of return we are trying to predict
    :param refit_every: the amount of consecutive days to predict using a single model
        this is essentially saying refit the model every x days
    :param expanding: the minimum amount of days of data to train on
        if rollingDays is passed then this should not be passed
        if this value is passed then the model will be trained with an expanding window of data
    :param rolling: the amount of rolling days to fit a model to
        if minTrainDays is passed then this should not be passed
    :return: a generator with each iteration containing a tuple of two SliceHolders of dates.
            Slice One: training indexes
            Slice Two: predicting indexes
    """

    if (eval_days < 1) or (refit_every < 1):
        raise ValueError('eval_days and/or refit_every must be greater than zero')
    if rolling is not None and (rolling < 1):
        raise ValueError('rolling must be greater than zero')
    if expanding is not None and (expanding < 1):
        raise ValueError('expanding must be greater than zero')
    if (not bool(expanding)) and (not bool(rolling)):
        raise ValueError('minTrainDays or rollingDays must be defined')
    if bool(expanding) & bool(rolling):
        raise ValueError('minTrainDays and rollingDays can not both be defined')

    dates: np.array = data_index.get_level_values(0).drop_duplicates().to_numpy()

    start_place = expanding if expanding else rolling
    # dont have to ceil this bc it wont matter with a < operator
    amount_of_loops: float = (len(dates) - start_place - eval_days) / refit_every

    i: int = 0
    while i < amount_of_loops:
        # .loc[] is inclusive in a slice, so everything here is inclusive
        train_end_index: int = (i * refit_every) + (start_place - 1)
        train_start_index: int = train_end_index - rolling + 1 if rolling else 0
        train_slice: SliceHolder = SliceHolder(dates[train_start_index], dates[train_end_index])

        predict_start_index: int = train_end_index + eval_days + 1
        predict_end_index: int = predict_start_index + refit_every - 1
        # accounting for when the ending predicted index is out of bounds on the last loop
        if predict_end_index >= len(dates) - 1:
            predict_end_index: int = len(dates) - 1

        predict_slice: SliceHolder = SliceHolder(dates[predict_start_index], dates[predict_end_index])

        i += 1
        yield train_slice, predict_slice
