from abc import ABC, abstractmethod


class BaseTear(ABC):
    """
    The base class for all tearsheets
    """

    def __init__(self):
        """
        empty constructor
        """

    @abstractmethod
    def compute(self) -> None:
        """
        method which makes the tearsheet
        :return: None
        """
        pass
