from xml.dom import minidom

from exporters.Exporter import Exporter
import xml.etree.ElementTree as ET


class ProjectExporter(Exporter):
    def __init__(self):
        super().__init__()

    def _generate_xml(self) -> str:
        root = ET.Element("composite_semantic_pattern")
        definition = ET.SubElement(root, "definition")

        project = self._airtable.get_record_by_formula('Project', '{ID}')
        fields = project.get('fields')

        system_name = ET.SubElement(definition, "system_name")
        system_name.text = fields.get('UI_Name')

        system_identifier = ET.SubElement(definition, "system_identifier")
        system_identifier.text = fields.get('ID')

        descriptions = ET.SubElement(definition, "descriptions")
        description = ET.SubElement(descriptions, "description")

        description_content = ET.SubElement(description, "description_content")
        description_content.text = fields.get('Description')

        description_type = ET.SubElement(description, "description_type")
        description_type.text = 'Scope Note'

        description_language = ET.SubElement(description, "description_language")
        description_language.text = 'English'

        namespace = ET.SubElement(definition, "namespace")
        namespace.text = fields.get('Namespace')

        semantic_context = ET.SubElement(root, "semantic_context")
        ontologies = ET.SubElement(semantic_context, "ontologies")
        for ontology in self._airtable.get_all_records_from_table('CRM Class'):
            record = ontology.get("fields")
            ontology_el = ET.SubElement(ontologies, "ontology")

            ontology_name = ET.SubElement(ontology_el, "ontology_name")
            ontology_name.text = record.get("ID")

            ontology_uri = ET.SubElement(ontology_el, "ontology_URI")
            ontology_uri.text = record.get("Subject")

            ontology_version = ET.SubElement(ontology_el, "ontology_version")
            ontology_version.text = record.get("CRM Version")

        namespaces = ET.SubElement(semantic_context, "namespaces")
        for namespace in self._airtable.get_all_records_from_table('NameSpaces'):
            record = namespace.get("fields")

            namespace_el = ET.SubElement(namespaces, "namespace")

            namespace_prefix = ET.SubElement(namespace_el, "namespace_prefix")
            namespace_prefix.text = record.get("Abbreviation")

            namespace_uri = ET.SubElement(namespace_el, "namespace_URI")
            namespace_uri.text = record.get("Namespace")

        components = ET.SubElement(root, "components")
        atomic_semantic_patterns = ET.SubElement(components, "atomic_semantic_patterns")
        for field in self._airtable.get_all_records_from_table("Field"):
            record = field.get("fields")
            atomic_semantic_pattern = ET.SubElement(atomic_semantic_patterns, "atomic_semantic_pattern")

            atomic_semantic_pattern_name = ET.SubElement(atomic_semantic_pattern, "atomic_semantic_pattern_name")
            atomic_semantic_pattern_name.text = record.get("UI_Name")

            atomic_semantic_pattern_URI = ET.SubElement(atomic_semantic_pattern, "atomic_semantic_pattern_URI")
            atomic_semantic_pattern_URI.text = record.get("URI")

        composite_semantic_patterns = ET.SubElement(components, "composite_semantic_patterns")

        composite_patterns = self._airtable.get_all_records_from_table("Model")
        composite_patterns.extend(self._airtable.get_all_records_from_table("Collection"))

        for pattern in composite_patterns:
            record = pattern.get("fields")
            composite_semantic_pattern = ET.SubElement(composite_semantic_patterns, "composite_semantic_pattern")

            composite_semantic_pattern_name = ET.SubElement(composite_semantic_pattern, "composite_semantic_pattern_name")
            composite_semantic_pattern_name.text = record.get("UI_Name")

            composite_semantic_pattern_URI = ET.SubElement(composite_semantic_pattern, "composite_semantic_pattern_URI")
            composite_semantic_pattern_URI.text = record.get("URI")

        rough_string = ET.tostring(root, "utf-8")
        reparsed = minidom.parseString(rough_string)

        return reparsed.toprettyxml(indent=4 * " ")

