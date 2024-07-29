import io
from typing import List

from pyairtable.api.types import RecordDict
from pyairtable.formulas import match
from rdflib import Graph, URIRef, Literal, RDF
from rdflib.namespace import Namespace

from ZellijData.AirTableConnection import AirTableConnection
from db import generate_airtable_schema, decrypt

CRM = Namespace("http://www.cidoc-crm.org/cidoc-crm/")


class TurtleTransformer:
    field: RecordDict
    crm_class: RecordDict

    def __init__(self, api_key: str, field_id: str):
        self.id = field_id
        self.api_key = api_key

        schemas, secretkey = generate_airtable_schema(api_key)
        airtable = AirTableConnection(decrypt(secretkey), api_key)
        self.field = (airtable.get_record_by_formula("Field", match({"ID": field_id}))
                      or airtable.get_record_by_id("Field", field_id))
        self.crm_class = airtable.get_record_by_id("CRM Class", self.field.get("fields", {}).get("Ontology_Scope")[0])

    def transform(self) -> io.BytesIO:
        graph = Graph()
        graph.bind("crm", CRM)
        graph.bind("rdf", RDF)

        total_path: str = (
                self.crm_class.get("fields", {}).get("Class_Nim", "")
                + self.field.get("fields", {}).get("Ontological_Long_Path", "")
        )

        parts: List[str] = total_path.split("->")
        uris = {}
        for idx in range(len(parts)):
            part = parts[idx]
            if idx % 2 == 0:
                if "[" in part:
                    identifiers = part.split('[')[1].split(']')[0]
                    uris[idx] = f"https://linked.art/example/{identifiers}"
                else:
                    uris[idx] = f"https://linked.art/example/{part}"

        for idx in range(len(parts)):
            current_part = parts[idx]

            if idx % 2 == 1:
                if parts[idx + 1] == "rdf:literal":
                    graph.add(
                        (
                            URIRef(uris[idx - 1]),
                            CRM[current_part],
                            Literal(self.field.get("fields", {}).get("System_Name", "").replace(" ", "_") + "_value")
                        )
                    )
                else:
                    graph.add((URIRef(uris[idx - 1]), CRM[current_part], URIRef(uris[idx + 1])))
            else:
                graph.add((URIRef(uris[idx]), RDF.type, CRM[current_part.split('[')[0]]))

        file = io.BytesIO()
        file.name = f"{self.field.get('fields', {}).get('System_Name', '').replace(' ', '_')}.ttl"
        file.write(graph.serialize(format="turtle").encode("utf-8"))
        file.seek(0)

        return file
