from abc import ABC
from typing import Union

from website.db import generate_airtable_schema, decrypt
from pyairtable.api.types import RecordDict
from pyairtable.formulas import match

from ZellijData.AirTableConnection import AirTableConnection


class Transformer(ABC):
    airtable: AirTableConnection
    field: RecordDict
    crm_class: Union[RecordDict, None] = None
    turtle: str

    def __init__(self, api_key: str, field_id: str):
        self.id = field_id
        self.api_key = api_key

        _, secretkey = generate_airtable_schema(api_key)
        self.airtable = AirTableConnection(decrypt(secretkey), api_key)
        self.field = self.airtable.get_record_by_formula(
            "Field", match({"ID": field_id})
        ) or self.airtable.get_record_by_id("Field", field_id)

        ontology_scope = self.field.get("fields", {}).get("Ontology_Scope")
        if isinstance(ontology_scope, list):
            ontology_scope = ontology_scope[0]

        try:
            self.crm_class = self.airtable.get_record_by_formula(
                "Ontology_Class", match({"ID": ontology_scope})
            )
        except Exception as e:
            print("Error getting Ontology Class: ", e)

        try:
            if self.crm_class is None:
                self.crm_class = self.airtable.get_record_by_id(
                    "CRM Class", ontology_scope
                )
        except Exception as e:
            print("Error getting Ontology Class: ", e)

        if self.crm_class is None:
            raise ValueError(f"Could not find CRM Class with ID {ontology_scope}")

    def get_field(self, table: str, field: str, value: str) -> Union[RecordDict, None]:
        try:
            return self.airtable.get_record_by_formula(table, match({field: value}))
        except Exception as e:
            print(f"Error getting {table}: ", e)
