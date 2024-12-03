import io
import xml.etree.ElementTree as ET
from typing import Literal, Union, List
from xml.dom import minidom

from pyairtable.api.types import RecordDict

from website.transformers.Transformer import Transformer


class X3MLTransformer(Transformer):
    def __init__(self, api_key: str, field_id: str):
        super().__init__(api_key, field_id)

    def get_field_or_default(self, field_name: str) -> str:
        return self.field.get("fields", {}).get(field_name, "")

    def get_major_number_of_part(self, part: str) -> str:
        if "[" not in part:
            return part

        return part.split("[")[-1].split("]")[0].split("_")[0]

    def _create_export_file(self, root: ET.Element) -> io.BytesIO:
        rough_string = ET.tostring(root, "utf-8")
        reparsed = minidom.parseString(rough_string)

        file = io.BytesIO()
        file.name = f"{self.get_field_or_default('System_Name').replace(' ', '_')}.x3ml"
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

    def _get_collection_name(self) -> str:
        collection = self.get_field_or_default("Collection_Deployed")
        collection_fields = self.get_records(collection, "Collection")
        if len(collection_fields) == 1:
            return collection_fields[0].get("fields", {}).get("ID", "")

        return ""

    def _add_mapping_domain(self, root: ET.Element) -> None:
        domain = ET.SubElement(root, "domain")
        ET.SubElement(domain, "source_node")
        target_node = ET.SubElement(domain, "target_node")
        entity = ET.SubElement(target_node, "entity")
        entity_type = ET.SubElement(entity, "type")
        entity_type.text = self._get_collection_name()

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

        if part == "rdf:literal":
            entity_type.text = "http://www.w3.org/2001/XMLSchema#string"
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

    def _populate_link(self, parent: ET.Element, field_id: str, form: Union[Literal["a"], Literal["b"]]) -> None:
        field: Union[RecordDict, None] = None
        if field_id == self.id:
            field = self.field
        else:
            records = self.get_records(field_id, "Field")
            field = records[0] if len(records) >= 1 else None

        if field is None:
            return

        link = ET.SubElement(parent, "link")
        link.attrib["template"] = field.get("fields", {}).get("ID", "")

        path = ET.SubElement(link, "path")
        ET.SubElement(path, "source_relation")
        target_relation = ET.SubElement(path, "target_relation")
        total_path = self.get_field_or_default(
            "Ontological_Long_Path"
        ) or self.get_field_or_default("Ontological_Path")

        parts = self._parse_ontological_path(total_path)
        # for form b take only the last two parts of the path
        if form == "b":
            parts = parts[-2:]

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

    def _populate_mappings(self, root: ET.Element, form: Union[Literal["a"], Literal["b"]]) -> None:
        mappings = ET.SubElement(root, "mappings")
        mapping = ET.SubElement(mappings, "mapping")
        self._add_mapping_domain(mapping)
        self._populate_link(mapping, self.id, form)

    def transform(self, form: Union[Literal["a"], Literal["b"]]) -> io.BytesIO:
        root = ET.Element("x3ml")

        self._populate_namespaces(root)
        self._populate_mappings(root, form)

        return self._create_export_file(root)
