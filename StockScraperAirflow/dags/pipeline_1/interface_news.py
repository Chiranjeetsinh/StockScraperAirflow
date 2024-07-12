from abc import ABCMeta, abstractmethod

class INews(metaclass=ABCMeta):

    @abstractmethod
    def fetch_data():
        "abstract method to fetch data"
