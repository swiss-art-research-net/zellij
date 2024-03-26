from abc import ABC, abstractmethod


class Exporter(ABC):
    @abstractmethod
    def initialize(self, selectedSchema: str, apiKey: str, item: str):
        pass

    @abstractmethod
    def export(self):
        pass
