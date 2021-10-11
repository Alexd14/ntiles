from abc import ABC, abstractmethod


class BaseTear(ABC):
    """
    The base class for all tearsheets
    """

    def __init__(self):
        """
        empty constructor
        """

    def compute_plot(self) -> None:
        """
        method which calculates stats and plots the data for the tearsheet
        :return: None
        """
        self.compute()
        self.plot()

    @abstractmethod
    def compute(self) -> None:
        """
        method which calculates stats for the tearsheet
        :return: None
        """
        pass

    @abstractmethod
    def plot(self) -> None:
        """
        method which plots data for the tearsheet
        :return: None
        """
        pass
