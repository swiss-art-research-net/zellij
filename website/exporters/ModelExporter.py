import xml.etree.ElementTree as ET
from xml.dom import minidom

from pyairtable.formulas import OR, EQUAL, STR_VALUE

from website.exporters.Exporter import Exporter


class ModelExporter(Exporter):
    def __init__(self):
        super().__init__()

    def _generate_xml(self) -> str:
        root = ET.Element("composite_semantic_pattern")
        definition = ET.SubElement(root, "definition")
        semantic_context = ET.SubElement(root, "semantic_context")
        pattern_context = ET.SubElement(root, "pattern_context")

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
                if not self._prefill_group.get(key, {}).get("exportable", False) and key != "KeyField":
                    continue

                if self._prefill_group.get(key, {}).get('name') == "UI_Name":
                    name = ET.SubElement(names, "name")
                    name.attrib["uri"] = "http://vocab.getty.edu/aat/300456628"
                    name_content = ET.SubElement(name, "name_content")
                    name_content.attrib["uri"] = "http://vocab.getty.edu/aat/300456619"
                    name_content_label = ET.SubElement(name_content, "name_content_label")
                    name_content_label.text = val

                    name_type = ET.SubElement(name, "name_type")
                    name_type.text = "UI_Name"

                    name_language = ET.SubElement(name, "name_language")
                    name_language.text = "English"

                if self._prefill_group.get(key, {}).get('name') == "System_Name":
                    system_name = ET.SubElement(definition, "system_name")
                    system_name.attrib["uri"] = "http://vocab.getty.edu/aat/300456630"
                    system_name_label = ET.SubElement(system_name, "system_name_label")
                    system_name_label.text = val

                if self._prefill_group.get(key, {}).get('name') == "Identifier":
                    system_identifier = ET.SubElement(definition, "system_identifier")
                    system_identifier.text = val

                if self._prefill_group.get(key, {}).get('name') == "Ontology_Context":
                    ontologies = ET.SubElement(semantic_context, "ontologies")
                    for record_id in val:
                        record = self._airtable.get_record_by_id('Ontology', record_id)
                        ontology = ET.SubElement(ontologies, "ontology")
                        ontology.attrib["uri"] = record.get("fields", {}).get("Namespace")

                        ontology_prefix = ET.SubElement(ontology, "ontology_prefix")
                        ontology_prefix.text = record.get("fields", {}).get("Prefix")

                        ontology_name = ET.SubElement(ontology, "ontology_name")
                        ontology_name.attrib["uri"] = "http://vocab.getty.edu/aat/300456628"
                        ontology_name_label = ET.SubElement(ontology_name, "ontology_name_label")
                        ontology_name_label.text = record.get("fields", {}).get("UI_Name")

                        ontology_version = ET.SubElement(ontology, "ontology_version")
                        ontology_version.attrib["uri"] = "http://vocab.getty.edu/aat/300456598"
                        ontology_version_label = ET.SubElement(ontology_version, "ontology_version_label")
                        ontology_version_label.text = record.get("fields", {}).get("Version")

                if key == "KeyField":
                    identifiers = ET.SubElement(definition, "identifiers")
                    identifier = ET.SubElement(identifiers, "identifier")

                    identifier_content = ET.SubElement(identifier, "identifier_content")
                    identifier_content.text = val

                    identifier_type = ET.SubElement(identifier, "identifier_type")
                    identifier_type.text = "unique identifier"

                if self._prefill_group.get(key, {}).get('name') == "Description":
                    descriptions = ET.SubElement(definition, "descriptions")
                    description = ET.SubElement(descriptions, "description")
                    description_content = ET.SubElement(description, "description_content")
                    description_content.attrib["uri"] = "http://vocab.getty.edu/aat/300456619"
                    description_content_label = ET.SubElement(description_content, "description_content_label")
                    description_content_label.text = val

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
                        ontology_class.attrib["uri"] = record.get("fields", {}).get("Subject")

                        class_name = ET.SubElement(ontology_class, "class_name")
                        class_name.attrib["uri"] = "http://vocab.getty.edu/aat/300456619"
                        class_name_label = ET.SubElement(class_name, "class_name_label")
                        class_name_label.text = record.get("fields", {}).get("ID")

                if self._prefill_group.get(key, {}).get('name') == "Model_NameSpaces":
                    semantic_context = ET.SubElement(root, "semantic_context")

                if self._prefill_group.get(key, {}).get('name') == "Project":
                    for record_id in val:
                        record = self._airtable.get_record_by_id('Project', record_id)

                        semantic_pattern_space = ET.SubElement(pattern_context, "semantic_pattern_space")
                        semantic_pattern_space.attrib["uri"] = record.get("fields", {}).get("Namespace")

                        semantic_pattern_space_name = ET.SubElement(semantic_pattern_space, "semantic_pattern_space_name")
                        semantic_pattern_space_name.attrib["uri"] = "http://vocab.getty.edu/aat/300456628"
                        semantic_pattern_space_name_label = ET.SubElement(semantic_pattern_space_name, "semantic_pattern_space_name_label")
                        semantic_pattern_space_name_label.text = record.get("fields", {}).get("UI_Name")

                if (self._prefill_group.get(key, {}).get('name') == "Fields_Expected_Resource_Model" or
                    self._prefill_group.get(key, {}).get('name') == "Fields_Expected_Collection_Model"
                ):
                    semantic_patterns_deployed_in = ET.SubElement(pattern_context, "semantic_patterns_deployed_in")
                    target_of = ET.SubElement(semantic_patterns_deployed_in, "target_of")

                    for field in self._airtable.get_multiple_records_by_formula('Field', OR(*list(map(lambda x: EQUAL(STR_VALUE(x), 'RECORD_ID()'), val)))):
                        fields = field.get('fields')

                        atomic_semantic_pattern = ET.SubElement(target_of, "atomic_semantic_pattern")
                        atomic_semantic_pattern.attrib["uri"] = fields.get("URI").strip()

                        atomic_semantic_pattern_name = ET.SubElement(atomic_semantic_pattern, "atomic_semantic_pattern_name")
                        atomic_semantic_pattern_name.attrib["uri"] = "http://vocab.getty.edu/aat/300456628"
                        atomic_semantic_pattern_name_label = ET.SubElement(atomic_semantic_pattern_name, "atomic_semantic_pattern_name_label")
                        atomic_semantic_pattern_name_label.text = fields.get("UI_Name")

                        composite_semantic_pattern_type = ET.SubElement(atomic_semantic_pattern, "composite_semantic_pattern_type")
                        composite_semantic_pattern_type.text = self._selected_scheme

                if self._prefill_group.get(key, {}).get('name') == "Model_Fields" or self._prefill_group.get(key, {}).get('name') == "Collection_Fields":
                    referenced_table = self._prefill_group.get(key, {}).get('name')
                    composition = ET.SubElement(root, "composition")

                    fields_to_populate = {}
                    fields_uris = []

                    for model_field in self._airtable.get_multiple_records_by_formula(referenced_table, OR(*list(map(lambda x: EQUAL(STR_VALUE(x), 'RECORD_ID()'), val)))):
                        pattern = ET.SubElement(composition, "pattern")

                        atomic_semantic_pattern_name = ET.SubElement(pattern, "atomic_semantic_pattern_name")
                        atomic_semantic_pattern_name.attrib["uri"] = "http://vocab.getty.edu/aat/300456628"
                        atomic_semantic_pattern_name_label = ET.SubElement(atomic_semantic_pattern_name, "atomic_semantic_pattern_name_label")
                        field_ui_names = model_field.get("fields", {}).get("Field_UI_Name")
                        atomic_semantic_pattern_name_label.text = field_ui_names[0] if len(field_ui_names) > 0 else ''

                        if len(model_field.get("fields", {}).get("Field")) > 0:
                            fields_to_populate[model_field.get("fields", {}).get("Field")[0]] = pattern
                            fields_uris.append(model_field.get("fields", {}).get("Field")[0])

                    for field in self._airtable.get_multiple_records_by_formula('Field', OR(*list(map(lambda x: EQUAL(STR_VALUE(x), 'RECORD_ID()'), fields_uris)))):
                        fields_to_populate[field['id']].attrib["uri"] = field.get("fields", {}).get("URI")

                if self._prefill_group.get(key, {}).get('name') == "Total_SparQL":
                    serialization = ET.SubElement(root, "serialization")
                    encodings_el = ET.SubElement(serialization, "encodings")
                    encoding = ET.SubElement(encodings_el, "encoding")

                    encoding_content = ET.SubElement(encoding, "encoding_content")
                    encoding_content.text = val

                    encoding_type = ET.SubElement(encoding, "encoding_type")
                    encoding_type.attrib["uri"] = "http://vocab.getty.edu/aat/300456634"
                    encoding_type_label = ET.SubElement(encoding_type, "encoding_type_label")
                    encoding_type_label.text = "rdf"

                    encoding_format = ET.SubElement(encoding, "encoding_format")
                    encoding_format.attrib["uri"] = "http://vocab.getty.edu/aat/300456635"
                    encoding_format_label = ET.SubElement(encoding_format, "encoding_format_label")
                    encoding_format_label.text = "sparql"

                if self._prefill_group.get(key, {}).get('name') == "x3ml":
                    serialization = ET.SubElement(root, "serialization")
                    encodings_el = ET.SubElement(serialization, "encodings")
                    encoding = ET.SubElement(encodings_el, "encoding")

                    encoding_content = ET.SubElement(encoding, "encoding_content")
                    encoding_content.text = val

                    encoding_type = ET.SubElement(encoding, "encoding_type")
                    encoding_type.attrib["uri"] = "http://vocab.getty.edu/aat/300266654"
                    encoding_type_label = ET.SubElement(encoding_type, "encoding_type_label")
                    encoding_type_label.text = "x3ml"

                    encoding_format = ET.SubElement(encoding, "encoding_format")
                    encoding_format.attrib["uri"] = "http://vocab.getty.edu/aat/300266654"
                    encoding_type_label = ET.SubElement(encoding_format, "encoding_format_label")
                    encoding_type_label.text = "xml"

                if self._prefill_group.get(key, {}).get('name') == "Version":
                    version_number = ET.SubElement(version_data, "version_number")
                    version_number.attrib["uri"] = "http://vocab.getty.edu/aat/300456598"
                    version_number_label = ET.SubElement(version_number, "version_number_label")
                    version_number_label.text = val
                if self._prefill_group.get(key, {}).get('name') == "Version_Date":
                    version_publication_date = ET.SubElement(version_data, "version_publication_date")
                    version_publication_date.attrib["uri"] = "http://vocab.getty.edu/aat/300456620"
                    version_publication_date_label = ET.SubElement(version_publication_date, "version_publication_date_label")
                    version_publication_date_label.text = val
                if self._prefill_group.get(key, {}).get('name') == "Last_Modified":
                    post_version_modification_date = ET.SubElement(version_data, "post_version_modification_date")
                    post_version_modification_date.attrib["uri"] = "http://vocab.getty.edu/aat/300456620"
                    post_version_modification_date_label = ET.SubElement(post_version_modification_date, "post_version_modification_date_label")
                    post_version_modification_date_label.text = val

                if self._prefill_group.get(key, {}).get('name') == "Authors":
                    creators = ET.SubElement(creation_data, "creators")
                    for record_id in val:
                        author = self._airtable.get_record_by_id('Actors', record_id)
                        creator = ET.SubElement(creators, "creator")
                        creator.attrib['uri'] = author.get("fields", {}).get("URI", '')

                        creator_name = ET.SubElement(creator, "creator_name")
                        creator_name.attrib["uri"] = "http://vocab.getty.edu/aat/300456619"
                        creator_name_label = ET.SubElement(creator_name, "creator_name_label")
                        creator_name_label.text = author.get("fields", {}).get("Name")

                if self._prefill_group.get(key, {}).get('name') == "Funders":
                    funder = ET.SubElement(funding, "funder")

                    for record_id in val:
                        funder_record = self._airtable.get_record_by_id('Institution', record_id)

                        funder_name = ET.SubElement(funder, "funder_name")
                        funder_name.attrib["uri"] = "http://vocab.getty.edu/aat/300456619"
                        funder_name_label = ET.SubElement(funder_name, "funder_name_label")
                        funder_name_label.text = funder_record.get("fields", {}).get("Name")

        rough_string = ET.tostring(root, "utf-8")
        reparsed = minidom.parseString(rough_string)

        return reparsed.toprettyxml(indent=4 * " ")
