import io
from itertools import chain
from typing import Dict, List, Union

from pyairtable.api.types import RecordDict
from pyairtable.formulas import match
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
    def __init__(self, api_key: str, field_id: str):
        super().__init__(api_key, field_id)
        total_path: str = self.field.get("fields", {}).get("Ontological_Long_Path", "")
        self.parts: List[str] = self._parse_ontological_path(total_path)
        self.uris = {-1: "subject"}
        self.populate_uris()

    def _parse_ontological_path(self, path: str) -> List[str]:
        long_discriminator = "-->"
        short_discriminator = "->"
        stripped_path = path.strip(short_discriminator).strip(long_discriminator)

        parts = stripped_path.split(long_discriminator)
        parts = list(map(lambda x: x.split(short_discriminator), parts))

        return list(chain.from_iterable(parts))

    def populate_uris(self):
        self.self_uri = self.get_field_or_default("ID").replace(".", "_")
        for idx, part in enumerate(self.parts):
            if idx % 2 == 0:
                continue

            if part == "rdf:literal":
                self.uris[idx] = part
                continue

            if part.startswith("xsl"):
                self.uris[idx] = self.self_uri
                continue

            if idx > 2 and self.get_major_number_of_part(
                self.parts[idx - 2]
            ) == self.get_major_number_of_part(part):
                self.uris[idx] = f"<{self.get_field_or_default('Set_Value')}>"
                continue

            collection = self.get_field_or_default("Collection_Deployed")

            collection_field = self.get_records(collection, "Collection")
            if len(collection_field) == 1:
                self.uris[idx] = (
                    collection_field[0]
                    .get("fields", {})
                    .get("ID", "")
                    .replace(".", "_")
                )
            else:
                self.uris[idx] = self.self_uri

    def get_field_or_default(self, field_name: str) -> str:
        return self.field.get("fields", {}).get(field_name, "")

    def get_major_number_of_part(self, part: str) -> str:
        if "[" not in part:
            return part

        return part.split("[")[-1].split("]")[0].split("_")[0]

    def upload(self):
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

        try:
            self.airtable.airtable.table(
                base_id=base_api_key, table_name="Field"
            ).update(base_field.get("id"), {"sparql_test": self.sparql})
        except Exception as e:
            print("Error uploading Sparql: ", e)
            raise e

    def create_where_pattern(self, optional=False):
        where_pattern = SPARQLGraphPattern(optional=optional)
        for idx in range(len(self.parts)):
            current_part = self.parts[idx]
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

        where_pattern.add_binding(Binding(f"?{self.self_uri}", "?value"))

        expected_value_type = self.get_field_or_default("Expected_Value_Type")
        if "rdf:literal" not in self.parts and expected_value_type not in [
            "Date",
            "Integer",
        ]:
            optional_label = SPARQLGraphPattern(optional=True)
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

    def transform(self, count: bool = False):
        if count:
            query = SPARQLSelectQuery()
            query.add_variables(["(COUNT(?value) as ?count)"])
        else:
            query = SPARQLSelectQuery(limit=100)
        self.add_prefixes(query)

        where_pattern = self.create_where_pattern()

        query.set_where_pattern(where_pattern)

        self.sparql = query.get_text()
        file = io.BytesIO()
        file.name = (
            f"{self.get_field_or_default('System_Name').replace(' ', '_')}.sparql"
        )
        file.write(self.sparql.encode("utf-8"))
        file.seek(0)

        return file
