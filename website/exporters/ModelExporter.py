import xml.etree.ElementTree as ET
from xml.dom import minidom

from pyairtable.formulas import match, OR, EQUAL, FIELD, STR_VALUE

from website.exporters.Exporter import Exporter


class ModelExporter(Exporter):
    def __init__(self):
        super().__init__()

    def _generate_xml(self) -> str:
        root = ET.Element("composite_semantic_pattern")
        definition = ET.SubElement(root, "definition")

        type_el = ET.SubElement(definition, "type")
        type_el.text = self._selected_scheme

        provenance = ET.SubElement(root, "provenance")
        version_data = ET.SubElement(provenance, "version_data")
        creation_data = ET.SubElement(provenance, "creation_data")
        funding = ET.SubElement(provenance, "funding")

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

                if self._prefill_group.get(key, {}).get('name') == "Fields_Expected_Resource_Model" or self._prefill_group.get(key, {}).get('name') == "Fields_Expected_Collection_Model":
                    pattern_context = ET.SubElement(root, "pattern_context")

                    semantic_patterns_deployed_in = ET.SubElement(pattern_context, "semantic_patterns_deployed_in")
                    target_of = ET.SubElement(semantic_patterns_deployed_in, "target_of")

                    for field in self._airtable.get_multiple_records_by_formula('Field', OR(*list(map(lambda x: EQUAL(STR_VALUE(x), 'RECORD_ID()'), val)))):
                        fields = field.get('fields')

                        atomic_semantic_pattern = ET.SubElement(target_of, "atomic_semantic_pattern")
                        atomic_semantic_pattern_name = ET.SubElement(atomic_semantic_pattern, "atomic_semantic_pattern_name")
                        atomic_semantic_pattern_name.text = fields.get("UI_Name")

                        atomic_semantic_pattern_uri = ET.SubElement(atomic_semantic_pattern, "atomic_semantic_pattern_URI")
                        atomic_semantic_pattern_uri.text = fields.get("URI").strip()

                        composite_semantic_pattern_type = ET.SubElement(atomic_semantic_pattern, "composite_semantic_pattern_type")
                        composite_semantic_pattern_type.text = self._selected_scheme

                if self._prefill_group.get(key, {}).get('name') == "Model_Fields" or self._prefill_group.get(key, {}).get('name') == "Collection_Fields":
                    referenced_table = self._prefill_group.get(key, {}).get('name')
                    composition = ET.SubElement(root, "composition")

                    for model_field in self._airtable.get_multiple_records_by_formula(referenced_table, OR(*list(map(lambda x: EQUAL(STR_VALUE(x), 'RECORD_ID()'), val)))):
                        pattern = ET.SubElement(composition, "pattern")

                        pattern_name = ET.SubElement(pattern, "pattern_name")
                        field_ui_names = model_field.get("fields", {}).get("Field_UI_Name")
                        pattern_name.text = field_ui_names[0] if len(field_ui_names) > 0 else ''

                        pattern_uri = ET.SubElement(pattern, "pattern_URI")
                        pattern_uri.text = "uri"

                if self._prefill_group.get(key, {}).get('name') == "Total_SparQL":
                    serialization = ET.SubElement(root, "serialization")
                    encodings_el = ET.SubElement(serialization, "encodings")
                    encoding = ET.SubElement(encodings_el, "encoding")

                    encoding_content = ET.SubElement(encoding, "encoding_content")
                    encoding_content.text = val

                    encoding_type = ET.SubElement(encoding, "encoding_type")
                    encoding_type.text = "rdf"

                    encoding_format = ET.SubElement(encoding, "encoding_format")
                    encoding_format.text = "sparql"

                if self._prefill_group.get(key, {}).get('name') == "x3ml":
                    serialization = ET.SubElement(root, "serialization")
                    encodings_el = ET.SubElement(serialization, "encodings")
                    encoding = ET.SubElement(encodings_el, "encoding")

                    encoding_content = ET.SubElement(encoding, "encoding_content")
                    encoding_content.text = val

                    encoding_type = ET.SubElement(encoding, "encoding_type")
                    encoding_type.text = "x3ml"

                    encoding_format = ET.SubElement(encoding, "encoding_format")
                    encoding_format.text = "xml"

                if self._prefill_group.get(key, {}).get('name') == "Version":
                    version_number = ET.SubElement(version_data, "version_number")
                    version_number.text = val
                if self._prefill_group.get(key, {}).get('name') == "Version_Date":
                    version_publication_date = ET.SubElement(version_data, "version_publication_date")
                    version_publication_date.text = val
                if self._prefill_group.get(key, {}).get('name') == "Last_Modified":
                    post_version_modification_date = ET.SubElement(version_data, "post_version_modification_date")
                    post_version_modification_date.text = val

                if self._prefill_group.get(key, {}).get('name') == "Authors":
                    creators = ET.SubElement(creation_data, "creators")
                    for record_id in val:
                        author = self._airtable.get_record_by_id('Actors', record_id)
                        creator = ET.SubElement(creators, "creator")

                        creator_name = ET.SubElement(creator, "creator_name")
                        creator_name.text = author.get("fields", {}).get("Name")

                        creator_URI = ET.SubElement(creator, "creator_URI")
                        creator_URI.text = author.get("fields", {}).get("URI", '')

                if self._prefill_group.get(key, {}).get('name') == "Funders":
                    funder = ET.SubElement(funding, "funder")

        rough_string = ET.tostring(root, "utf-8")
        reparsed = minidom.parseString(rough_string)

        return reparsed.toprettyxml(indent=4 * " ")
