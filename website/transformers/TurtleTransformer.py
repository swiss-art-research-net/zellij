import io
from typing import List, Dict, Union

from pyairtable.api.types import RecordDict
from pyairtable.formulas import match
from rdflib import Graph, URIRef, Literal, RDF
from rdflib.namespace import Namespace, DefinedNamespaceMeta

from ZellijData.AirTableConnection import AirTableConnection
from db import generate_airtable_schema, decrypt

CRM = Namespace("http://www.cidoc-crm.org/cidoc-crm/")


class TurtleTransformer:
    airtable: AirTableConnection
    field: RecordDict
    crm_class: RecordDict

    def __init__(self, api_key: str, field_id: str):
        self.id = field_id
        self.api_key = api_key

        schemas, secretkey = generate_airtable_schema(api_key)
        self.airtable = AirTableConnection(decrypt(secretkey), api_key)
        self.field = (self.airtable.get_record_by_formula("Field", match({"ID": field_id}))
                      or self.airtable.get_record_by_id("Field", field_id))
        self.crm_class = self.airtable.get_record_by_id("CRM Class",
                                                        self.field.get("fields", {}).get("Ontology_Scope")[0])

    def transform(self) -> io.BytesIO:
        graph = Graph()

        namespaces: Dict[str, Union[Namespace, DefinedNamespaceMeta]] = {}
        for namespace in self.airtable.get_all_records_from_table("NameSpaces"):
            prefix = namespace.get("fields", {}).get("Abbreviation", "")
            uri = namespace.get("fields", {}).get("Namespace", "")
            namespaces[prefix] = Namespace(uri)
            graph.bind(prefix, namespaces[prefix])
        namespaces["rdf"] = RDF
        graph.bind("rdf", RDF)

        total_path: str = self.field.get("fields", {}).get("Ontological_Long_Path", "")

        parts: List[str] = total_path.lstrip("->").split("->")
        uris = {-1: self.crm_class.get("fields", {}).get("Class_Ur_Instance").strip("<>")}

        for idx in range(len(parts)):
            part = parts[idx]
            if idx % 2 == 1:
                if "[" in part:
                    class_identifier = part.split('[')[0].split("_")[0]
                    crm_class = self.airtable.get_record_by_formula("CRM Class", match({"Class_Nim": class_identifier}))

                    if crm_class is not None:
                        instance_modifier = crm_class.get("fields", {}).get("Instance Modifier", class_identifier)
                        uris[idx] = (
                            f'{self.crm_class.get("fields", {}).get("Instance Root", "")}' +
                            f'{instance_modifier}/' +
                            f'{part.split("[")[1].split("]")[0]}'
                         )
                    else:
                        instance_root = self.crm_class.get("fields", {}).get("Instance Root", "")
                        uris[idx] = (
                            f'{instance_root}{"/" if instance_root[-1] != "/" else ""}' +
                            f'{part.split("[")[1].split("]")[0]}'
                        )
                else:
                    uris[idx] = f"https://linked.art/example/{part}"

        for idx in range(len(parts)):
            current_part = parts[idx]
            namespace = namespaces[current_part.split(":")[0] if ":" in current_part else "crm"]
            ns_class = current_part.split(":")[1] if ":" in current_part else current_part

            if idx % 2 == 0:
                if parts[idx + 1] == "rdf:literal":
                    graph.add(
                        (
                            URIRef(uris[idx - 1]),
                            namespace[ns_class],
                            Literal(self.field.get("fields", {}).get("System_Name", "").replace(" ", "_") + "_value")
                        )
                    )
                else:
                    graph.add((URIRef(uris[idx - 1]), namespace[ns_class], URIRef(uris[idx + 1])))
            else:
                if current_part == "rdf:literal":
                    continue
                graph.add((URIRef(uris[idx]), RDF.type, namespace[ns_class.split('[')[0]]))

        file = io.BytesIO()
        file.name = f"{self.field.get('fields', {}).get('System_Name', '').replace(' ', '_')}.ttl"
        file.write(graph.serialize(format="turtle").encode("utf-8"))
        file.seek(0)

        return file
