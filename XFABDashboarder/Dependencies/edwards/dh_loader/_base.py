from abc import ABCMeta, abstractmethod


class Base(metaclass=ABCMeta):
    """Base loader"""
    @abstractmethod
    def get_data(self):
        pass
