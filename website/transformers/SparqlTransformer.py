import io
from typing import Dict, List, Union

from rdflib import RDF, Namespace
from rdflib.namespace import DefinedNamespaceMeta
from website.transformers.Transformer import Transformer
from SPARQLBurger.SPARQLQueryBuilder import Prefix, SPARQLGraphPattern, SPARQLSelectQuery, Triple


class SparqlTransformer(Transformer):
    def __init__(self, api_key: str, field_id: str):
        super().__init__(api_key, field_id)

    def transform(self):
        query = SPARQLSelectQuery()

        namespaces: Dict[str, Union[Namespace, DefinedNamespaceMeta]] = {}
        namespaces_records = []

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
            uri = namespace.get("fields", {}).get("Namespace", "")
            namespaces[prefix] = Namespace(uri)
            query.add_prefix(prefix=Prefix(prefix=prefix, namespace=namespaces[prefix]))
        namespaces["rdf"] = RDF
        query.add_prefix(prefix=Prefix(prefix=prefix, namespace=RDF))

        total_path: str = self.field.get("fields", {}).get(
            "Ontological_Long_Path", ""
        ) or self.field.get("fields", {}).get("Ontological_Path", "")

        discriminator = "-->" if "-->" in total_path else "->"
        parts: List[str] = total_path.lstrip(discriminator).split(discriminator)
        uris = {
            -1: self.crm_class.get("fields", {}).get("Class_Ur_Instance", "").strip("<>")
        }

        for idx in range(len(parts)):
            part = parts[idx]
            if idx % 2 == 1:
                if "[" in part:
                    class_identifier = part.split("[")[0].split("_")[0]
                    if ":" in class_identifier:
                        class_identifier = class_identifier.split(":")[1]
                    crm_class = None

                    if crm_class is None:
                        crm_class = self.get_field(
                            "Ontology_Class", "Identifier", class_identifier
                        )

                    if crm_class is None:
                        crm_class = self.get_field(
                            "Ontology_Class", "Class_Nim", class_identifier
                        )

                    if crm_class is None:
                        crm_class = self.get_field(
                            "CRM Class", "Class_Nim", class_identifier
                        )

                    if crm_class is None:
                        crm_class = self.get_field(
                            "CRM Class", "Identifier", class_identifier
                        )

                    if crm_class is not None:
                        print("Found class with identifier: ", class_identifier)
                        instance_modifier = crm_class.get("fields", {}).get(
                            "Instance Modifier"
                        ) or crm_class.get("fields", {}).get(
                            "Instance_Modifier", class_identifier
                        )

                        instance_root = crm_class.get("fields", {}).get(
                            "Instance Root"
                        ) or crm_class.get("fields", {}).get("Instance_Root", "")
                        uris[idx] = (
                            f"{instance_root}"
                            + f"{instance_modifier}/"
                            + f'{part.split("[")[1].split("]")[0]}'
                        )
                    else:
                        print(
                            "Could not find class with identifier: ",
                            class_identifier,
                            "using instance root",
                        )
                        instance_root = self.crm_class.get("fields", {}).get(
                            "Instance Root"
                        ) or self.crm_class.get("fields", {}).get("Instance_Root", "")
                        uris[idx] = (
                            f'{instance_root}{"/" if instance_root[-1] != "/" else ""}'
                            + f'{part.split("[")[1].split("]")[0]}'
                        )
                else:
                    uris[idx] = f"https://linked.art/example/{part}"

        where_pattern = SPARQLGraphPattern()
        for idx in range(len(parts)):
            current_part = parts[idx]
            namespace = namespaces[
                current_part.split(":")[0] if ":" in current_part else "crm"
            ]
            ns_class = (
                current_part.split(":")[1] if ":" in current_part else current_part
            )
            ns_class = ns_class.strip()

            if idx % 2 == 0:
                if parts[idx + 1] == "rdf:literal":
                    where_pattern.add_triples([
                        Triple(
                            subject=uris[idx - 1],
                            predicate=namespace[ns_class],
                            object=self.field.get("fields", {}).get("System_Name", "").replace(" ", "_") + "_value",
                        )
                    ])
                else:
                    where_pattern.add_triples([
                        Triple(
                            subject=uris[idx - 1],
                            predicate=namespace[ns_class],
                            object=uris[idx + 1],
                        )
                    ])
            else:
                if current_part == "rdf:literal":
                    continue
                where_pattern.add_triples([
                    Triple(
                        subject=uris[idx],
                        predicate=RDF.type,
                        object=namespace[ns_class.split("[")[0]],
                    )
                ])

        query.set_where_pattern(where_pattern)

        file = io.BytesIO()
        file.name = f"{self.field.get('fields', {}).get('System_Name', '').replace(' ', '_')}.sparql"
        file.write(query.get_text().encode("utf-8"))
        file.seek(0)

        return file
