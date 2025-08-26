import io
import re
from typing import Dict, List, Union

import yaml
from pyairtable.api.types import RecordDict
from pyairtable.formulas import EQ, OR, match, quoted

from website.db import decrypt, dict_gen_one, generate_airtable_schema, get_db
from website.transformers.SparqlTransformer import SparqlTransformer
from website.transformers.Transformer import Transformer
from ZellijData.AirTableConnection import AirTableConnection


class ResearchSpaceTransformer(Transformer):
    def _fetch_scraper_definition(self):
        database = get_db()
        c = database.cursor()
        c.execute(
            "SELECT * FROM AirTableDatabases WHERE dbaseapikey=%s", (self.api_key,)
        )
        self.scraper_definition = dict_gen_one(c)
        c.close()

    def __init__(
        self, api_key: str, pattern: str, model_id: str, field_id: Union[str, None]
    ):
        self.model_id = model_id
        self.field_id = field_id
        self.api_key = api_key

        schemas, secretkey = generate_airtable_schema(api_key)
        self.airtable = AirTableConnection(decrypt(secretkey), api_key)
        self.pattern = pattern
        self._fetch_scraper_definition()

        schema = schemas[pattern]
        for tablename, fieldlist in schema.items():
            if not isinstance(fieldlist, dict):
                continue
            if "GroupBy" in fieldlist:
                self.field_table = tablename
                self.field_table_group_by = fieldlist["GroupBy"]
            else:
                self.pattern = tablename

    def _populate_namespaces(self, data: Dict) -> None:
        records = self.airtable.get_all_records_from_table("Ontology")
        data["namespaces"] = {}

        for record in records:
            prefix = record["fields"]["Prefix"]
            namespace = record["fields"]["Namespace"]

            data["namespaces"][prefix] = namespace

    def _get_fields(self) -> List[RecordDict]:
        if self.field_id:
            self.field = self.airtable.get_record_by_formula(
                "Field", match({"ID": self.field_id})
            ) or self.airtable.get_record_by_id("Field", self.field_id)

            return [self.field]
        elif self.model_id:
            models = self.get_records(self.model_id, self.pattern)
            if len(models) == 1:
                self.model = models[0]

            model_fields_ids = list(
                map(
                    lambda x: x["fields"]["Field"][0]
                    if len(x["fields"]["Field"][0]) > 0
                    else x["fields"]["Field"],
                    self.airtable.get_multiple_records_by_formula(
                        self.field_table,
                        f'SEARCH("{self.model_id}",{{{self.field_table_group_by}}})',
                    ),
                )
            )

            return list(
                filter(
                    lambda field: len(
                        field.get("fields", {}).get("Collection_Deployed", "")
                    )
                    > 0,
                    self.airtable.get_multiple_records_by_formula(
                        "Field",
                        OR(
                            *list(
                                map(
                                    lambda x: EQ(quoted(x), "RECORD_ID()"),
                                    model_fields_ids,
                                )
                            )
                        ),
                    ),
                )
            )

        return []

    def _populate_field(self, field: RecordDict, field_data: Dict) -> None:
        field_data["id"] = field["fields"]["System_Name"]
        field_data["label"] = field["fields"]["UI_Name"]
        field_data["description"] = field["fields"]["Description"]

        datatypes = {
            "reference model": "xsd:anyURI",
            "uri": "xsd:anyURI",
            "concept": "xsd:anyURI",
            "collection": "xsd:anyURI",
            "integer": "xsd:integer",
            "date": "xsd:date",
            # "dateTime": "xsd:dateTime",
            "string": "xsd:string",
            # "": "xsd:langString",
            # "": "xsd:boolean",
            # "": "xsd:double",
            # "": "xsd:decimal",
        }

        sparql_transformer = SparqlTransformer(self.api_key, field["id"])
        sparql_transformer.transform()

        field_data["datatype"] = datatypes.get(
            field["fields"]["Expected_Value_Type"].lower(), "xsd:anyURI"
        )
        field_data["queries"] = [{"select": sparql_transformer.sparql}]

    def _populate_fields(self, data: Dict) -> None:
        data["fields"] = []

        fields = self._get_fields()
        for field in fields:
            field_data = {}
            self._populate_field(field, field_data)
            data["fields"].append(field_data)

    def _get_file_name(self) -> str:
        if self.field_id:
            return self.field["fields"]["System_Name"]
        elif self.model_id:
            return self.model["fields"]["System_Name"]

        return ""

    def _create_export_file(self, document: str) -> io.BytesIO:
        file = io.BytesIO()
        file.name = f"{self._get_file_name()}.yml"
        file.write(document.encode("utf-8"))
        file.seek(0)

        return file

    def upload(self) -> None:
        column_name = "rs"
        table_name = "Field" if self.field_id else self.pattern
        record_id = self.field_id if self.field_id else self.model_id

        base_api_key = self.api_key
        if self.scraper_definition is not None and self.scraper_definition["fieldbase"]:
            base_api_key = self.scraper_definition["fieldbase"]

        base_field = None
        try:
            base_field = self.airtable.airtable.table(
                base_id=base_api_key, table_name=table_name
            ).first(formula=match({"ID": record_id}))
        except Exception as e:
            print("Error getting Field: ", e)

        if base_field is None:
            table = self.airtable.airtable.table(
                base_id=self.api_key, table_name=table_name
            )
            base_field = table.first(
                formula=match({"ID": record_id})
            ) or self.airtable.get_record_by_id("Field", record_id)

            if base_field is not None:
                base_api_key = self.api_key
            else:
                raise ValueError(
                    "Field already exists in Field table, but not in Field table in the Field Base"
                )

        try:
            self.airtable.airtable.table(
                base_id=base_api_key, table_name=table_name
            ).update(base_field.get("id"), {column_name: self.content})
        except Exception as e:
            print("Error uploading RS Definition: ", e)
            raise e

    def transform(self) -> io.BytesIO:
        data = {"prefix": "", "container": ""}

        self._populate_namespaces(data)
        self._populate_fields(data)
        width = 0
        for field in data["fields"]:
            description = field["description"].encode().decode("unicode_escape").strip()
            if len(description) > width:
                width = len(description)
            field["description"] = description
            for query in field["queries"]:
                query["select"] = query["select"].encode().decode("unicode_escape")
                match = re.search(r"\bSELECT\b", query["select"], re.IGNORECASE)

                if match:
                    final_query = (
                        query["select"][match.start() :]
                        .encode()
                        .decode("unicode_escape")
                        .strip()
                    )
                    query["select"] = final_query

        def str_presenter(dumper, data):
            if "\n" in data:  # if the string contains newlines
                # The literal block style (|) is used for better readability.
                return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
            return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="'")

        yaml.add_representer(str, str_presenter, Dumper=yaml.SafeDumper)
        print(width)
        self.content = yaml.safe_dump(
            data,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
            width=10000,
        )
        self.content = self.content.replace("\\\n", " ")
        self.content = self.content.replace("\\n", "\n\t\t")
        self.content = self.content.replace("\\", " ")

        return self._create_export_file(self.content)
