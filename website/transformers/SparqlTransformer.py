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

        for idx, part in enumerate(parts):
            if idx % 2 == 1:
                if "[" in part:
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
                        uris[idx] = self.get_field_or_default("ID").replace(".", "_")
                else:
                    uris[idx] = part

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
                                object=f'?{self.get_field_or_default("ID").replace(".", "_")}',
                            )
                        ]
                    )
                else:
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
                                    object=f"?{uris[idx + 1]}",
                                )
                            ]
                        )
            else:
                if current_part == "rdf:literal":
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

        if self.get_field_or_default("ID").replace(".", "_") not in uris.values():
            where_pattern.add_binding(
                Binding(f"?{uris[1]}", f'?{self.get_field_or_default("ID").replace(".", "_")}')
            )

        where_pattern.add_binding(
            Binding(f'?{self.get_field_or_default("ID").replace(".", "_")}', "?value")
        )

        if "rdf:literal" not in parts:
            optional_label = SPARQLGraphPattern(optional=True)
            optional_label.add_triples(
                [
                    Triple(
                        subject=f'?{self.get_field_or_default("ID").replace(".", "_")}',
                        predicate="rdfs:label",
                        object=f'?{self.get_field_or_default("ID").replace(".", "_")}_label',
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
