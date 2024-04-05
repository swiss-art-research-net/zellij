import io
import xml.etree.ElementTree as ET
from xml.dom import minidom

from ZellijData.AirTableConnection import AirTableConnection
from website.datasources import get_prefill
from website.db import generate_airtable_schema, decrypt
from website.exporters.Exporter import Exporter


class ModelExporter(Exporter):
    _results: list
    _prefill_group: dict
    _item: str
    _airtable: AirTableConnection

    def __init__(self):
        super().__init__()

    def _generate_xml(self) -> str:
        root = ET.Element("composite_semantic_pattern")
        definition = ET.SubElement(root, "definition")

        type_el = ET.SubElement(definition, "type")
        type_el.text = "Model"

        names = ET.SubElement(definition, "names")
        for result in self._results:
            if result.get("KeyField") != self._item:
                continue
            for key, val in result.items():
                if not self._prefill_group.get(key, {}).get("exportable", False):
                    continue

                if self._prefill_group.get(key, {}).get('name') == "UI_Name":
                    name = ET.SubElement(names, "name")
                    nameContent = ET.SubElement(name, "name_content")
                    nameContent.text = val

                    name_type = ET.SubElement(name, "name_type")
                    name_type.text = "UI_Name"

                    name_language = ET.SubElement(name, "name_language")
                    name_language.text = "English"

                if self._prefill_group.get(key, {}).get('name') == "Identifier":
                    system_identifier = ET.SubElement(definition, "system_identifier")
                    system_identifier.text = val

                if self._prefill_group.get(key, {}).get('name') == "Description":
                    descriptions = ET.SubElement(definition, "descriptions")
                    description = ET.SubElement(descriptions, "description")
                    description_content = ET.SubElement(description, "description_content")
                    description_content.text = val

                    description_type = ET.SubElement(description, "description_type")
                    description_type.text = "Scope Note"

                    description_language = ET.SubElement(description, "description_language")
                    description_language.text = "English"

                    description_preference = ET.SubElement(description, "description_preference")
                    description_preference.text = "Preferred"

                if self._prefill_group.get(key, {}).get('name') == "Ontological_Scope":
                    ontological_scopes = ET.SubElement(definition, "ontological_scopes")
                    for record_id in val:
                        record = self._airtable.get_record_by_id('CRM Class', record_id)

                        ontology_class = ET.SubElement(ontological_scopes, "ontology_class")
                        class_name = ET.SubElement(ontology_class, "class_name")
                        class_name.text = record.get("fields", {}).get("ID")

                        class_uri = ET.SubElement(ontology_class, "class_URI")
                        class_uri.text = record.get("fields", {}).get("Subject")

                if self._prefill_group.get(key, {}).get('name') == "Model_NameSpaces":
                    semantic_context = ET.SubElement(root, "semantic_context")

                if self._prefill_group.get(key, {}).get('name') == "Fields_Expected_Resource_Model":
                    pattern_context = ET.SubElement(root, "pattern_context")

                    semantic_patterns_deployed_in = ET.SubElement(pattern_context, "semantic_patterns_deployed_in")
                    target_of = ET.SubElement(semantic_patterns_deployed_in, "target_of")

                    for record_id in val:
                        field = self._airtable.get_record_by_id('Field', record_id)

                        atomic_semantic_pattern = ET.SubElement(target_of, "atomic_semantic_pattern")
                        atomic_semantic_pattern_name = ET.SubElement(atomic_semantic_pattern, "atomic_semantic_pattern_name")
                        atomic_semantic_pattern_name.text = field.get("fields", {}).get("UI_Name")

                        atomic_semantic_pattern_uri = ET.SubElement(atomic_semantic_pattern, "atomic_semantic_pattern_URI")
                        atomic_semantic_pattern_uri.text = field.get("fields", {}).get("URI").strip()

                        composite_semantic_pattern_type = ET.SubElement(atomic_semantic_pattern, "composite_semantic_pattern_type")
                        composite_semantic_pattern_type.text = "Model"

                if self._prefill_group.get(key, {}).get('name') == "Model_Fields":
                    composition = ET.SubElement(root, "composition")

                    for record_id in val:
                        model_field = self._airtable.get_record_by_id('Model_Fields', record_id)
                        pattern = ET.SubElement(composition, "pattern")

                        pattern_name = ET.SubElement(pattern, "pattern_name")
                        field_ui_names = model_field.get("fields", {}).get("Field_UI_Name")
                        pattern_name.text = field_ui_names[0] if len(field_ui_names) > 0 else ''

                        pattern_uri = ET.SubElement(pattern, "pattern_URI")
                        pattern_uri.text = "uri"

        rough_string = ET.tostring(root, "utf-8")
        reparsed = minidom.parseString(rough_string)

        return reparsed.toprettyxml(indent=4 * " ")

    def initialize(self, selected_scheme: str, api_key: str, item: str):
        schemas, secretkey = generate_airtable_schema(api_key)
        self._airtable = AirTableConnection(decrypt(secretkey), api_key)

        schema = schemas[selected_scheme]
        _, prefill_group, _ = get_prefill(api_key, schema.get("id"))

        self._results = self._airtable.getListOfGroups(schema)
        self._prefill_group = prefill_group
        self._item = item

    def export(self) -> io.BytesIO:
        file = io.BytesIO()

        content = self._generate_xml()

        file.write(content.encode('utf-8'))
        file.seek(0)

        return file
