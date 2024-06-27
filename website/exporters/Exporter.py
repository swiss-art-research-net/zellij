import io
from abc import ABC, abstractmethod

from ZellijData.AirTableConnection import AirTableConnection
from website.datasources import get_prefill
from website.db import generate_airtable_schema, decrypt


class Exporter(ABC):
    _results: list
    _prefill_group: dict
    _prefill_data: dict
    _item: str
    _selected_scheme: str
    _airtable: AirTableConnection
    _name: str

    @abstractmethod
    def _generate_xml(self) -> str:
        pass

    def initialize(self, selected_scheme: str, api_key: str, item: str):
        schemas, secretkey = generate_airtable_schema(api_key)
        self._airtable = AirTableConnection(decrypt(secretkey), api_key)

        if item is None:
            return

        schema = schemas[selected_scheme]
        prefill_data, prefill_group, _ = get_prefill(api_key, schema.get("id"))

        self._selected_scheme = selected_scheme
        self._results = self._airtable.getListOfGroups(schema)
        self._prefill_group = prefill_group
        self._prefill_data = prefill_data
        self._item = item

    def export(self) -> io.BytesIO:
        file = io.BytesIO()

        content = self._generate_xml()

        file.write(content.encode('utf-8'))
        file.seek(0)

        return file

    def get_name(self):
        return self._name
