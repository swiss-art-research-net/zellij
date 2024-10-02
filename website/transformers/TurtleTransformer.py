import io
from typing import List, Dict, Union

from website.db import get_db, generate_airtable_schema, decrypt, dict_gen_one
from pyairtable.api.types import RecordDict
from pyairtable.formulas import match, OR
from rdflib import Graph, URIRef, Literal, RDF
from rdflib.namespace import Namespace, DefinedNamespaceMeta

from ZellijData.AirTableConnection import AirTableConnection
from website.db import generate_airtable_schema, decrypt

CRM = Namespace("http://www.cidoc-crm.org/cidoc-crm/")


class TurtleTransformer:
    airtable: AirTableConnection
    field: RecordDict
    crm_class: Union[RecordDict, None]
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

        self.crm_class: Union[RecordDict, None] = None

        try:
            if self.crm_class is None:
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

    def upload(self):
        database = get_db()
        c = database.cursor()
        c.execute(
            "SELECT * FROM AirTableDatabases WHERE dbaseapikey=%s", (self.api_key,)
        )
        existing = dict_gen_one(c)
        c.close()

        escaped_ttl_lines = list(
            filter(
                lambda line: not line.startswith("@") and line != "",
                self.turtle.splitlines(),
            )
        )
        escaped_ttl_lines[0] = " ".join(escaped_ttl_lines[0].split(" ")[1:])
        escaped_ttl = "\n".join(escaped_ttl_lines)

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
                raise ValueError(
                    "Field already exists in Field table, but not in Field table in the Field Base"
                )

        try:
            self.airtable.airtable.table(
                base_id=base_api_key, table_name="Field"
            ).update(base_field.get("id"), {"Turtle_Representation": escaped_ttl})
        except Exception as e:
            print("Error uploading Turtle: ", e)
            raise e

    def get_class(self, table: str, field: str, value: str) -> Union[RecordDict, None]:
        try:
            return self.airtable.get_record_by_formula(
                table, match({field: value})
            )
        except Exception as e:
            print(f"Error getting {table}: ", e)

    def transform(self) -> io.BytesIO:
        graph = Graph()

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
            graph.bind(prefix, namespaces[prefix])
        namespaces["rdf"] = RDF
        graph.bind("rdf", RDF)

        total_path: str = self.field.get("fields", {}).get(
            "Ontological_Long_Path", ""
        ) or self.field.get("fields", {}).get("Ontological_Path", "")

        discriminator = "-->" if "-->" in total_path else "->"
        parts: List[str] = total_path.lstrip(discriminator).split(discriminator)
        uris = {
            -1: self.crm_class.get("fields", {}).get("Class_Ur_Instance").strip("<>")
        }

        for idx in range(len(parts)):
            part = parts[idx]
            if idx % 2 == 1:
                if "[" in part:
                    class_identifier = part.split("[")[0].split("_")[0]
                    crm_class = None

                    if crm_class is None:
                        crm_class = self.get_class("Ontology_Class", "Identifier", class_identifier)

                    if crm_class is None:
                        crm_class = self.get_class("Ontology_Class", "Class_Nim", class_identifier)

                    if crm_class is None:
                        crm_class = self.get_class("CRM Class", "Class_Nim", class_identifier)

                    if crm_class is None:
                        crm_class = self.get_class("CRM Class", "Identifier", class_identifier)

                    if crm_class is not None:
                        print("Found class with identifier: ", class_identifier)
                        instance_modifier = crm_class.get("fields", {}).get(
                            "Instance Modifier", class_identifier
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
                        print("Could not find class with identifier: ", class_identifier, "using instance root")
                        instance_root = self.crm_class.get("fields", {}).get(
                            "Instance Root"
                        ) or self.crm_class.get("fields", {}).get("Instance_Root", "")
                        uris[idx] = (
                            f'{instance_root}{"/" if instance_root[-1] != "/" else ""}'
                            + f'{part.split("[")[1].split("]")[0]}'
                        )
                else:
                    uris[idx] = f"https://linked.art/example/{part}"

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
                    graph.add(
                        (
                            URIRef(uris[idx - 1]),
                            namespace[ns_class],
                            Literal(
                                self.field.get("fields", {})
                                .get("System_Name", "")
                                .replace(" ", "_")
                                + "_value"
                            ),
                        )
                    )
                else:
                    graph.add(
                        (
                            URIRef(uris[idx - 1]),
                            namespace[ns_class],
                            URIRef(uris[idx + 1]),
                        )
                    )
            else:
                if current_part == "rdf:literal":
                    continue
                graph.add(
                    (URIRef(uris[idx]), RDF.type, namespace[ns_class.split("[")[0]])
                )

        self.turtle = graph.serialize(format="turtle")
        file = io.BytesIO()
        file.name = f"{self.field.get('fields', {}).get('System_Name', '').replace(' ', '_')}.ttl"
        file.write(self.turtle.encode("utf-8"))
        file.seek(0)

        return file
