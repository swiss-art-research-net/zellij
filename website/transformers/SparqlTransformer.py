import io
from typing import Dict, List, Union

from pyairtable.api.types import RecordDict
from rdflib import RDF, Namespace
from rdflib.namespace import DefinedNamespaceMeta
from SPARQLBurger.SPARQLQueryBuilder import (
    Binding,
    Prefix,
    SPARQLGraphPattern,
    SPARQLSelectQuery,
    Triple,
)

from website.transformers.Transformer import Transformer


class SparqlTransformer(Transformer):
    def __init__(self, api_key: str, field_id: str):
        super().__init__(api_key, field_id)

    def get_field_or_default(self, field_name: str) -> str:
        return self.field.get("fields", {}).get(field_name, "")

    def get_major_number_of_part(self, part: str) -> str:
        if "[" not in part:
            return part

        return part.split("[")[-1].split("]")[0].split("_")[0]

    def transform(self):
        query = SPARQLSelectQuery(limit=100)

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

        total_path: str = self.field.get("fields", {}).get("Ontological_Path", "")

        discriminator = "-->" if "-->" in total_path else "->"
        parts: List[str] = total_path.lstrip(discriminator).split(discriminator)
        uris = {
            -1: (self.crm_class or {})
            .get("fields", {})
            .get("Class_Ur_Instance", "")
            .strip("<>")
        }

        self_uri = self.get_field_or_default("ID").replace(".", "_")
        for idx, part in enumerate(parts):
            if idx % 2 == 0:
                continue

            if part == "rdf:literal":
                uris[idx] = part
                continue

            if part.startswith("xsl"):
                uris[idx] = self_uri
                continue

            if idx > 2 and self.get_major_number_of_part(parts[idx - 2]) == self.get_major_number_of_part(part):
                uris[idx] = f"<{self.get_field_or_default('Set_Value')}>"
                continue

            collection = self.get_field_or_default("Collection_Deployed")

            collection_field = self.get_records(collection, "Collection")
            if len(collection_field) == 1:
                uris[idx] = (
                    collection_field[0]
                    .get("fields", {})
                    .get("ID", "")
                    .replace(".", "_")
                )
            else:
                uris[idx] = self_uri

        where_pattern = SPARQLGraphPattern()
        for idx in range(len(parts)):
            current_part = parts[idx]
            namespace = current_part.split(":")[0] if ":" in current_part else "crm"
            ns_class = (
                current_part.split(":")[1] if ":" in current_part else current_part
            ).strip()

            if idx % 2 == 0:
                if parts[idx + 1] == "rdf:literal":
                    where_pattern.add_triples(
                        [
                            Triple(
                                subject=f"?{uris[idx - 1]}",
                                predicate=f"{namespace}:{ns_class}",
                                object=f'?{self_uri}',
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
                                object=f"?{uris[idx + 1]}",
                            )
                        ]
                    )
                else:
                    where_pattern.add_triples(
                        [
                            Triple(
                                subject=f"?{uris[idx - 1]}",
                                predicate=f"{namespace}:{ns_class}",
                                object=f"?{uris[idx + 1]}" if "<" not in uris[idx + 1] else uris[idx + 1],
                            )
                        ]
                    )
            else:
                if current_part == "rdf:literal" or "<" in uris[idx]:
                    continue

                where_pattern.add_triples(
                    [
                        Triple(
                            subject=f"?{uris[idx]}",
                            predicate="a",
                            object=f"{namespace}:{ns_class.split('[')[0]}",
                        )
                    ]
                )

        if self_uri not in uris.values() and "rdf:literal" not in uris.values():
            where_pattern.add_binding(
                Binding(f"?{uris[1]}", f'?{self_uri}')
            )

        where_pattern.add_binding(
            Binding(f'?{self_uri}', "?value")
        )

        expected_value_type = self.get_field_or_default("Expected_Value_Type")
        if "rdf:literal" not in parts and expected_value_type not in ["Date", "Integer"]:
            optional_label = SPARQLGraphPattern(optional=True)
            optional_label.add_triples(
                [
                    Triple(
                        subject=f'?{self_uri}',
                        predicate="rdfs:label",
                        object=f'?{self_uri}_label',
                    )
                ]
            )

            where_pattern.add_nested_graph_pattern(optional_label)

        query.set_where_pattern(where_pattern)

        file = io.BytesIO()
        file.name = (
            f"{self.get_field_or_default('System_Name').replace(' ', '_')}.sparql"
        )
        file.write(query.get_text().encode("utf-8"))
        file.seek(0)

        return file
