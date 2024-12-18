import io
import xml.etree.ElementTree as ET
from typing import List, Literal, Union
from xml.dom import minidom

from pyairtable.api.types import RecordDict
from pyairtable.formulas import match

from website.db import decrypt, generate_airtable_schema
from website.transformers.Transformer import Transformer
from ZellijData.AirTableConnection import AirTableConnection


class X3MLTransformer(Transformer):
    literal_uris = {
        "xsd:date": "http://www.w3.org/2001/XMLSchema#dateTime",
        "xsd:dateTime": "http://www.w3.org/2001/XMLSchema#dateTime",
        "rdf:literal": "http://www.w3.org/2001/XMLSchema#string",
    }

    def __init__(
        self, api_key: str, pattern: str, model_id: str, field_id: Union[str, None]
    ):
        self.model_id = model_id
        self.field_id = field_id
        self.api_key = api_key
        self.field: Union[RecordDict, None] = None

        _, secretkey = generate_airtable_schema(api_key)
        self.airtable = AirTableConnection(decrypt(secretkey), api_key)
        self.pattern = pattern

        if field_id:
            self.field = self.airtable.get_record_by_formula(
                "Field", match({"ID": field_id})
            ) or self.airtable.get_record_by_id("Field", field_id)
            if self.field:
                self.file_name = (
                    self.field.get("fields", {})
                    .get("System_Name", "")
                    .replace(" ", "_")
                )

        self.model = None
        if model_id and not field_id:
            self._fetch_model()
            if self.model:
                self.file_name = (
                    self.model.get("fields", {})
                    .get("System_Name", "")
                    .replace(" ", "_"),
                )

    def _fetch_model(self):
        models = self.get_records(self.model_id, self.pattern)
        if len(models) == 1:
            self.model = models[0]

    def _fetch_model_fields(self):
        return self.airtable.get_all_records_from_table("Field")

    def get_major_number_of_part(self, part: str) -> str:
        if "[" not in part:
            return part

        return part.split("[")[-1].split("]")[0].split("_")[0]

    def _create_export_file(self, root: ET.Element) -> io.BytesIO:
        rough_string = ET.tostring(root, "utf-8")
        reparsed = minidom.parseString(rough_string)

        file = io.BytesIO()
        file.name = f"{self.file_name}.x3ml"
        file.write(reparsed.toprettyxml(indent=4 * " ").encode("utf-8"))
        file.seek(0)

        return file

    def upload(self):
        pass

    def _parse_ontological_path(self, path: str) -> List[str]:
        discriminator = "-->" if "-->" in path else "->"
        stripped_path = path.strip(discriminator)

        return stripped_path.split(discriminator)

    def _extract_entity_variable(self, entity: str) -> str:
        if "[" not in entity:
            return ""

        return entity.split("[")[-1].split("]")[0]

    def _populate_namespaces(self, root: ET.Element) -> None:
        records = self.airtable.get_all_records_from_table("Ontology")
        namespaces = ET.SubElement(root, "namespaces")

        for record in records:
            namespace = ET.SubElement(namespaces, "namespace")
            namespace.attrib["prefix"] = record["fields"]["Prefix"]
            namespace.attrib["uri"] = record["fields"]["Namespace"]

    def _get_collection_name(self, collection_id: str) -> str:
        collection_fields = self.get_records(collection_id, "Collection")
        if len(collection_fields) == 1:
            return collection_fields[0].get("fields", {}).get("ID", "")

        return ""

    def _add_mapping_domain(
        self, root: ET.Element, template: str, parts: Union[List[str], None] = None
    ) -> None:
        domain = ET.SubElement(root, "domain")
        domain.attrib["template"] = template
        source_node = ET.SubElement(domain, "source_node")
        target_node = ET.SubElement(domain, "target_node")
        entity = ET.SubElement(target_node, "entity")
        entity_type = ET.SubElement(entity, "type")
        entity_type.text = template

        if parts:
            relationship = ET.SubElement(source_node, "relationship")
            relationship.text = parts[0]
            self._self_populate_entity_node(entity, parts[-1])
        else:
            instance_generator = ET.SubElement(entity, "instance_generator")
            instance_generator.attrib["name"] = "UUID"

    def _create_instance_generator_arg(
        self, parent: ET.Element, name: str, type: str, content: str
    ) -> None:
        arg = ET.SubElement(parent, "arg")
        arg.attrib["name"] = name
        arg.attrib["type"] = type
        arg.text = content

    def _self_populate_entity_node(self, parent: ET.Element, part: str) -> None:
        entity = ET.SubElement(parent, "entity")
        entity_type = ET.SubElement(entity, "type")
        entity_instance_generator = ET.SubElement(entity, "instance_generator")

        if part in ["rdf:literal", "xsd:date", "xsd:dateTime", "xsd:time"]:
            entity_type.text = self.literal_uris[part]
            entity_instance_generator.attrib["name"] = "Literal"
            self._create_instance_generator_arg(
                entity_instance_generator, "text", "xpath", "text()"
            )
            self._create_instance_generator_arg(
                entity_instance_generator, "language", "constant", "en"
            )
        else:
            entity.attrib["variable"] = self._extract_entity_variable(part)
            entity_type.text = part.split("[")[0] if "[" in part else part
            entity_instance_generator.attrib["name"] = "UUID"

    def _add_field(
        self,
        parent: ET.Element,
        field_id: Union[str, RecordDict],
        form: Union[Literal["a"], Literal["b"]],
        first_part: bool = True,
    ) -> List[str]:
        field: Union[RecordDict, None] = None
        if field_id == self.field_id:
            field = self.field
        elif not isinstance(field_id, str):
            field = field_id
        else:
            records = self.get_records(field_id, "Field")
            field = records[0] if len(records) >= 1 else None

        if field is None:
            return []

        link = ET.SubElement(parent, "link")
        if form == "a":
            link.attrib["template"] = field.get("fields", {}).get("ID", "")
        else:
            link.attrib["template"] = self._get_collection_name(
                field.get("fields", {}).get("Collection_Deployed", "")
            )

        path = ET.SubElement(link, "path")
        ET.SubElement(path, "source_relation")
        target_relation = ET.SubElement(path, "target_relation")
        total_path = field.get("fields", {}).get("Ontological_Long_Path") or field.get(
            "fields", {}
        ).get("Ontological_Path")

        if not total_path:
            return []

        parts = self._parse_ontological_path(total_path)
        # for form b take only the last two parts of the path
        if form == "b":
            parts = parts[0:2] if first_part else parts[-2:]

        for idx, part in enumerate(parts):
            # if it is even then it is a relation else is an entity
            if idx % 2 == 0:
                relationship = ET.SubElement(target_relation, "relationship")
                relationship.text = part
            else:
                if idx == len(parts) - 1:
                    range = ET.SubElement(link, "range")
                    ET.SubElement(range, "source_node")
                    target_node = ET.SubElement(range, "target_node")
                    self._self_populate_entity_node(target_node, part)
                else:
                    self._self_populate_entity_node(target_relation, part)

        return parts

    def _populate_mappings(
        self, root: ET.Element, form: Union[Literal["a"], Literal["b"]]
    ) -> None:
        mappings = ET.SubElement(root, "mappings")
        mapping = ET.SubElement(mappings, "mapping")

        if form == "a":
            if self.field_id and self.field:
                self._add_mapping_domain(
                    mapping,
                    self._get_collection_name(
                        self.field.get("fields", {}).get("Collection_Deployed", "")
                    ),
                )
                self._add_field(mapping, self.field_id, form)
            elif self.model_id and self.model:
                self._add_mapping_domain(
                    mapping, self.model.get("fields", {}).get("ID", "")
                )
                fields = self._fetch_model_fields()

                for field in fields:
                    self._add_field(mapping, field, form)
            else:
                print("No model or field found")
        else:
            if self.model is None:
                self._fetch_model()

            assert self.model is not None

            self._add_mapping_domain(
                mapping, self.model.get("fields", {}).get("ID", "")
            )
            if not self.field or not self.field_id:
                return

            domain_parts = self._add_field(mapping, self.field_id, form, True)

            model_mapping = ET.SubElement(mappings, "mapping")
            self._add_mapping_domain(
                model_mapping,
                self._get_collection_name(
                    self.field.get("fields", {}).get("Collection_Deployed", "")
                ),
                domain_parts,
            )
            self._add_field(model_mapping, self.field_id, form, False)

    def transform(self, form: Union[Literal["a"], Literal["b"]]) -> io.BytesIO:
        root = ET.Element("x3ml")

        if form != "a" and form != "b":
            raise ValueError("Form must be either 'a' or 'b'")

        self._populate_namespaces(root)
        self._populate_mappings(root, form)

        return self._create_export_file(root)
