import io
from itertools import chain
from typing import Dict, List, Union

from pyairtable.api.types import RecordDict
from pyairtable.formulas import OR, match
from rdflib import RDF, Namespace
from rdflib.namespace import DefinedNamespaceMeta

from SPARQLBurger.SPARQLQueryBuilder import (
    Binding,
    Prefix,
    SPARQLGraphPattern,
    SPARQLSelectQuery,
    Triple,
)
from website.db import dict_gen_one, get_db
from website.transformers.Transformer import Transformer


class SparqlTransformer(Transformer):
    query: SPARQLSelectQuery

    def __init__(
        self, api_key: str, field_id: str, simple=False, model=None, model_id=None
    ):
        if simple:
            super().__init_simple__(api_key, field_id)
            return

        super().__init__(api_key, field_id)
        total_path: str = self.field.get("fields", {}).get(
            "Ontology_Long_Path",
            self.field.get("fields", {}).get("Ontology_Long_Path", ""),
        )
        self.parts: List[str] = self._parse_ontological_path(total_path)
        self.uris = {-1: "subject"}
        self.populate_uris(model=model, model_id=model_id)

    def _parse_ontological_path(self, path: str) -> List[str]:
        long_discriminator = "-->"
        short_discriminator = "->"
        stripped_path = path.strip(short_discriminator).strip(long_discriminator)

        parts = stripped_path.split(long_discriminator)
        parts = list(map(lambda x: x.split(short_discriminator), parts))

        return list(chain.from_iterable(parts))

    def populate_uris(
        self, model: Union[str, None] = None, model_id: Union[str, None] = None
    ):
        self.self_uri = (
            self.get_field_or_default("ID").replace(".", "_").replace("-", "_")
        )
        for idx, part in enumerate(self.parts):
            if idx % 2 == 0:
                continue

            if part == "rdf:literal":
                self.uris[idx] = part
                continue

            if part.startswith("xsl"):
                self.uris[idx] = self.self_uri
                continue

            if (
                idx > 2
                and self.get_major_number_of_part(self.parts[idx - 2])
                == self.get_major_number_of_part(part)
                and self.get_field_or_default("Set_Value")
            ):
                self.uris[idx] = f"<{self.get_field_or_default('Set_Value')}>"
                continue

            collection = []
            if model is not None and model_id is not None:
                if model == "Collection":
                    collection_field = None
                    collection = self.get_records(model_id, "Collection")
                else:
                    collection_field = self.airtable.get_record_by_formula(
                        "Model_Fields",
                        OR(
                            match(
                                {
                                    "Field": self.id,
                                    "Model": model_id,
                                }
                            ),
                            match(
                                {
                                    "Field": self.get_field_or_default("ID"),
                                    "Model": model_id,
                                }
                            ),
                        ),
                    )

                if collection_field is not None:
                    field_name = (
                        "Collection_Specific_Part_of_Collection"
                        if model == "Collection"
                        else "Model_Specific_Part_of_Collection"
                    )

                    collection_ids = collection_field.get("fields").get(field_name, [])
                    collection = self.get_records(collection_ids, "Collection")

            if len(collection) == 0 and idx == 1:
                self.uris[idx] = self.self_uri
            elif len(collection) == 1 and idx == 1:
                self.uris[idx] = (
                    collection[0].get("fields", {}).get("ID", "").replace(".", "_")
                )
            elif 1 < idx < len(self.parts) - 1:
                self.uris[idx] = self.number_to_variable(part)
            else:
                self.uris[idx] = self.self_uri

        for key, value in self.uris.items():
            self.uris[key] = value.replace(".", "_").replace("-", "_")

    def get_field_or_default(self, field_name: str) -> str:
        return self.field.get("fields", {}).get(field_name, "")

    def get_major_number_of_part(self, part: str) -> str:
        if "[" not in part:
            return part

        return part.split("[")[-1].split("]")[0].split("_")[0]

    def number_to_variable(self, part: str) -> str:
        if "[" not in part:
            return part

        return part.split(sep="[")[-1].split("]")[0].replace(".", "_")

    def upload(self):
        if self.query is None or self.query.where is None:
            return

        database = get_db()
        c = database.cursor()
        c.execute(
            "SELECT * FROM AirTableDatabases WHERE dbaseapikey=%s", (self.api_key,)
        )
        existing = dict_gen_one(c)
        c.close()

        base_api_key = self.api_key
        if existing is not None and existing["fieldbase"] is not None:
            base_api_key = existing["fieldbase"]

        base_field = None
        try:
            base_field = self.airtable.airtable.table(
                base_id=base_api_key, table_name="Field"
            ).first(formula=match({"ID": self.field.get("fields").get("ID")}))
        except Exception as e:
            print("Error getting Field: ", e)

        if base_field is None:
            base_field = self.airtable.airtable.table(
                base_id=self.api_key, table_name="Field"
            ).first(formula=match({"ID": self.field.get("fields").get("ID")}))

            if base_field is not None:
                base_api_key = self.api_key
            else:
                print("Field not found in Field table")
                raise ValueError(
                    "Field already exists in Field table, but not in Field table in the Field Base"
                )

        where_text: str = self.query.where.get_text()
        where_text = (
            where_text.strip("\n").removeprefix("{").removesuffix("}").strip("\n ")
        )

        try:
            self.airtable.airtable.table(
                base_id=base_api_key, table_name="Field"
            ).update(base_field.get("id"), {"SparQL": where_text})
        except Exception as e:
            print("Error uploading Sparql: ", e)
            raise e

    def get_class_uri(self, model: str, model_id: str):
        records = self.get_records(model_id, model)

        if not records:
            print(f"Could not find CRM Class with ID {model_id}")
            return None

        record = records[0].get("fields")
        if "Ontological_Scope_URI" in record:
            uris = record.get("Ontological_Scope_URI")
            return uris[0] if isinstance(uris, list) else uris
        elif "Ontology_Scope" in record:
            try:
                classes = self.get_records(record["Ontology_Scope"], "Ontology_Class")
            except Exception:
                classes = self.get_records(record["Ontology_Scope"], "CRM Class")
            uris = classes[0].get("fields").get("URI")
            return uris[0] if isinstance(uris, list) else uris

        return None

    def create_model_where(
        self,
        model: Union[str, None] = None,
        model_id: Union[str, None] = None,
        get_label=False,
    ):
        where_pattern = SPARQLGraphPattern()

        if model_id and model:
            class_uri = self.get_class_uri(model, model_id)
            if class_uri:
                where_pattern.add_triples(
                    [Triple(subject="?subject", predicate="a", object=f"<{class_uri}>")]
                )
            if get_label:
                where_pattern.add_triples(
                    [
                        Triple(
                            subject="?subject",
                            predicate="rdfs:label",
                            object="?labels",
                        )
                    ]
                )

        return where_pattern

    def create_where_pattern(
        self,
        model: Union[str, None] = None,
        model_id: Union[str, None] = None,
        optional=False,
        start=0,
    ):
        where_pattern = SPARQLGraphPattern(optional=optional)

        if model_id and model:
            class_uri = self.get_class_uri(model, model_id)
            if class_uri:
                where_pattern.add_triples(
                    [Triple(subject="?subject", predicate="a", object=f"<{class_uri}>")]
                )

        for idx, current_part in enumerate(self.parts):
            if idx < start:
                continue

            namespace = current_part.split(":")[0] if ":" in current_part else "crm"
            ns_class = (
                current_part.split(":")[1] if ":" in current_part else current_part
            ).strip()

            if idx % 2 == 0:
                if self.parts[idx + 1] == "rdf:literal":
                    where_pattern.add_triples(
                        [
                            Triple(
                                subject=f"?{self.uris[idx - 1]}",
                                predicate=f"{namespace}:{ns_class}",
                                object=f"?{self.self_uri}",
                            )
                        ]
                    )
                    continue

                if idx == 0:
                    where_pattern.add_triples(
                        [
                            Triple(
                                subject="?subject",
                                predicate=f"{namespace}:{ns_class}",
                                object=f"?{self.uris[idx + 1]}",
                            )
                        ]
                    )
                else:
                    where_pattern.add_triples(
                        [
                            Triple(
                                subject=f"?{self.uris[idx - 1]}",
                                predicate=f"{namespace}:{ns_class}",
                                object=f"?{self.uris[idx + 1]}"
                                if "<" not in self.uris[idx + 1]
                                else self.uris[idx + 1],
                            )
                        ]
                    )
            else:
                if current_part == "rdf:literal" or "<" in self.uris[idx]:
                    continue

                where_pattern.add_triples(
                    [
                        Triple(
                            subject=f"?{self.uris[idx]}",
                            predicate="a",
                            object=f"{namespace}:{ns_class.split('[')[0]}",
                        )
                    ]
                )

        if (
            self.self_uri not in self.uris.values()
            and "rdf:literal" not in self.uris.values()
        ):
            where_pattern.add_binding(Binding(f"?{self.uris[1]}", f"?{self.self_uri}"))

        if not optional:
            where_pattern.add_binding(Binding(f"?{self.self_uri}", "?value"))

        expected_value_type = self.get_field_or_default("Expected_Value_Type")
        if (
            not model_id
            and not model
            and "rdf:literal" not in self.parts
            and expected_value_type
            not in [
                "Date",
                "Integer",
            ]
        ):
            optional_label = SPARQLGraphPattern(optional=(not optional))
            optional_label.add_triples(
                [
                    Triple(
                        subject=f"?{self.self_uri}",
                        predicate="rdfs:label",
                        object=f"?{self.self_uri}_label",
                    )
                ]
            )

            where_pattern.add_nested_graph_pattern(optional_label)

        if self.get_field_or_default("Set_Value") and start == 0:
            where_pattern.add_triples(
                [
                    Triple(
                        subject=f"?{self.self_uri}",
                        predicate="crm:P2_has_type",
                        object=f"<{self.get_field_or_default('Set_Value')}>",
                    )
                ]
            )

        return where_pattern

    def add_prefixes(self, query: SPARQLSelectQuery):
        namespaces: Dict[str, Union[Namespace, DefinedNamespaceMeta]] = {}
        namespaces_records: List[RecordDict] = []
        try:
            namespaces_records.extend(
                self.airtable.get_all_records_from_table("NameSpaces")
            )
        except Exception as e:
            print("Error getting namespaces: ", e)

        try:
            namespaces_records.extend(
                self.airtable.get_all_records_from_table("Ontology")
            )
        except Exception as e:
            print("Error getting namespaces: ", e)

        for namespace in namespaces_records:
            prefix = namespace.get("fields", {}).get(
                "Abbreviation", ""
            ) or namespace.get("fields", {}).get("Prefix", "")

            if prefix in namespaces:
                continue

            uri: str = namespace.get("fields", {}).get("Namespace", "")
            namespaces[prefix] = Namespace(uri)
            query.add_prefix(prefix=Prefix(prefix=prefix, namespace=namespaces[prefix]))

        namespaces["rdf"] = RDF
        query.add_prefix(prefix=Prefix(prefix="rdf", namespace=RDF))

    def transform(
        self,
        count: bool = False,
        model: Union[str, None] = None,
        model_id: Union[str, None] = None,
        upload: bool = False,
    ):
        if count:
            query = SPARQLSelectQuery(limit=1)
            query.add_variables(["(COUNT(?value) as ?count)"])
        else:
            query = SPARQLSelectQuery(limit=100)
        self.add_prefixes(query)

        if upload:
            where_pattern = self.create_where_pattern()
        else:
            where_pattern = self.create_where_pattern(model=model, model_id=model_id)

        query.set_where_pattern(where_pattern)

        self.query = query
        self.sparql = query.get_text()
        file = io.BytesIO()
        file.name = f"{self.get_field_or_default('System_Name').replace(' ', '_')}{'_count' if count else ''}.rq"
        file.write(self.sparql.encode("utf-8"))
        file.seek(0)

        return file
