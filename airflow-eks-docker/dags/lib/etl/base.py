# ApiETL abstract base

from abc import ABC, abstractmethod

class ApiETL(ABC):
    """
    Abstract Base Class for API ETL processes.
    """

    @abstractmethod
    def extract(self, api_url: str, params: dict) -> str:
        pass

    @abstractmethod
    def transform(self, data: dict) -> dict:
        pass

    @abstractmethod
    def load(self, dataframes: dict) -> None:
        pass

    # @abstractmethod
    # def run(self) -> None:
    #     pass

