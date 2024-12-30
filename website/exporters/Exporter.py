import io
from abc import ABC, abstractmethod
from typing import List, Union

from pyairtable import Table
from pyairtable.api.types import RecordDict
from pyairtable.formulas import match

from website.datasources import get_prefill
from website.db import decrypt, generate_airtable_schema
from ZellijData.AirTableConnection import AirTableConnection


class Exporter(ABC):
    _results: list
    _prefill_group: dict
    _prefill_data: dict
    _item: str
    _selected_scheme: str
    _airtable: AirTableConnection
    _name: str
    _schema: dict
    tables: List[Table]
    value_types_terms = {
        "string": "http://vocab.getty.edu/aat/300456619",
        "date": "http://vocab.getty.edu/aat/300456620",
        "integer": "http://vocab.getty.edu/aat/300456621",
        "controlled terms": "http://vocab.getty.edu/aat/300456622",
        "geojson": "http://vocab.getty.edu/aat/300456623",
        "file/bitstream": "http://vocab.getty.edu/aat/300456624",
        "reference model": "http://vocab.getty.edu/aat/300456625",
        "collection": "http://vocab.getty.edu/aat/300456626",
        "annotation data type": "http://vocab.getty.edu/aat/300456627",
        "concept": "http://vocab.getty.edu/aat/300435691",
        "uri": " https://vocab.getty.edu/aat/300404629",
    }

    @abstractmethod
    def _generate_xml(self) -> str:
        pass

    def initialize(self, selected_scheme: str, api_key: str, item: str):
        schemas, secretkey = generate_airtable_schema(api_key)
        self._airtable = AirTableConnection(decrypt(secretkey), api_key)

        self.tables = self._airtable.airtable.base(api_key).tables()

        if item is None:
            return self

        self._schema = schemas[selected_scheme]
        prefill_data, prefill_group, _ = get_prefill(api_key, self._schema.get("id"))

        self._selected_scheme = selected_scheme
        self._results = self._airtable.getListOfGroups(self._schema)
        self._prefill_group = prefill_group
        self._prefill_data = prefill_data
        self._item = item
        self._name = item

        return self

    def export(self) -> io.BytesIO:
        file = io.BytesIO()

        content = self._generate_xml()

        file.write(content.encode("utf-8"))
        file.seek(0)

        return file

    def get_name(self):
        if self._name is None:
            return None
        return self._name

    def get_schema(self):
        return self._schema

    def get_records(self, item: Union[str, List[str]], table: str) -> List[RecordDict]:
        table_schema = self._airtable.airtable.table(
            table_name=table, base_id=self._airtable.airTableBaseAPI
        ).schema()
        records = []
        if isinstance(item, str):
            if "," in item:
                items = item.split(", ")

                for record in items:
                    records.append(
                        self._airtable.get_record_by_formula(
                            table, match({table_schema.primary_field_id: record})
                        )
                    )
            elif "rec" in item:
                records.append(self._airtable.get_record_by_id(table, item))
            else:
                records.append(
                    self._airtable.get_record_by_formula(
                        table, match({table_schema.primary_field_id: item})
                    )
                )
        else:
            for record in item:
                records.append(self._airtable.get_record_by_id(table, record))

        return list(filter(lambda x: x, records))

    def contains_table(self, table_name: str) -> bool:
        return any([x.name == table_name for x in self.tables])
