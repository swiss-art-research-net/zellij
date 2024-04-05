from ZellijData.AirTableConnection import AirTableConnection
from exporters.Exporter import Exporter


class ModelExporter(Exporter):
    _results: list
    _prefill_group: dict
    _item: str
    _airtable: AirTableConnection

    def __init__(self):
        super().__init__()

    def _generate_xml(self) -> str:
        pass
