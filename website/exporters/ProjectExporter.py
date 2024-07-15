from xml.dom import minidom

from website.exporters.Exporter import Exporter
import xml.etree.ElementTree as ET


class ProjectExporter(Exporter):
    def __init__(self):
        super().__init__()

    def _generate_xml(self) -> str:
        root = ET.Element("semantic_space")
        definition = ET.SubElement(root, "definition")

        project = self._airtable.get_record_by_formula('Project', '{ID}')
        fields = project.get('fields')

        root.attrib["uri"] = fields.get("Namespace")

        system_name = ET.SubElement(definition, "system_name")
        system_name.attrib["uri"] = "http://vocab.getty.edu/aat/300456630"
        system_name_label = ET.SubElement(system_name, "system_name_label")
        system_name_label.text = fields.get('System_Name')

        names = ET.SubElement(definition, "names")
        name = ET.SubElement(names, "name")
        name.attrib["uri"] = "http://vocab.getty.edu/aat/300456630"
        name_content = ET.SubElement(name, "name_label")
        name_content.text = fields.get('UI_Name')
        self._name = fields.get('UI_Name')

        name_type = ET.SubElement(name, "name_type")
        name_type.text = 'UI Name'

        name_language = ET.SubElement(name, "name_language")
        name_language.text = 'English'

        system_identifier = ET.SubElement(definition, "system_identifier")
        system_identifier.attrib["uri"] = "http://vocab.getty.edu/aat/300456619"
        system_identifier_label = ET.SubElement(system_identifier, "system_identifier_label")
        system_identifier_label.text = fields.get('ID')

        descriptions = ET.SubElement(definition, "descriptions")
        description = ET.SubElement(descriptions, "description")

        description_content = ET.SubElement(description, "description_content")
        description_content.attrib["uri"] = "http://vocab.getty.edu/aat/300456619"
        description_content_label = ET.SubElement(description_content, "description_content_label")
        description_content_label.text = fields.get('Description')

        description_type = ET.SubElement(description, "description_type")
        description_type.text = 'Scope Note'

        description_language = ET.SubElement(description, "description_language")
        description_language.text = 'English'

        namespace = ET.SubElement(definition, "namespace")
        namespace.attrib["uri"] = "http://vocab.getty.edu/aat/300456599"
        namespace_label = ET.SubElement(namespace, "namespace_label")
        namespace_label.text = fields.get('Namespace')

        semantic_context = ET.SubElement(root, "semantic_context")
        ontologies = ET.SubElement(semantic_context, "ontologies")
        for ontology in self._airtable.get_all_records_from_table('Ontology'):
            record = ontology.get("fields")
            ontology_el = ET.SubElement(ontologies, "ontology")
            ontology_el.attrib["uri"] = record.get("Namespace")

            ontology_name = ET.SubElement(ontology_el, "ontology_name")
            ontology_name.text = record.get("UI_Name")

            ontology_version = ET.SubElement(ontology_el, "ontology_version")
            ontology_version.attrib["uri"] = "http://vocab.getty.edu/aat/300456598"

            ontology_version_label = ET.SubElement(ontology_version, "ontology_version_label")
            ontology_version_label.text = record.get("Version")

        namespaces = ET.SubElement(semantic_context, "namespaces")
        for namespace in self._airtable.get_all_records_from_table('NameSpaces'):
            record = namespace.get("fields")

            namespace_el = ET.SubElement(namespaces, "namespace")
            namespace_el.attrib["uri"] = record.get("Namespace")

            namespace_prefix = ET.SubElement(namespace_el, "namespace_prefix")
            namespace_prefix.text = record.get("Abbreviation")

        dataspace_context = ET.SubElement(semantic_context, "dataspace_context")
        services = ET.SubElement(dataspace_context, "services")
        for service in self._airtable.get_all_records_from_table('Service'):
            service_fields = service.get("fields")

            if len(service_fields) == 0:
                continue

            service_el = ET.SubElement(services, "service")
            service_name = ET.SubElement(service_el, "service_name")
            service_name.text = service_fields.get("UI_Name")

            service_access_point = ET.SubElement(service_el, "service_access_point")
            service_access_point.text = service_fields.get("Access_Point")

            service_protocol = ET.SubElement(service_el, "service_protocol")
            service_protocol.text = service_fields.get("Protocol")

            service_type = ET.SubElement(service_el, "service_type")
            service_type.text = service_fields.get("Service_Type")

        components = ET.SubElement(root, "components")
        atomic_semantic_patterns = ET.SubElement(components, "atomic_semantic_patterns")
        for field in self._airtable.get_all_records_from_table("Field"):
            record = field.get("fields")
            atomic_semantic_pattern = ET.SubElement(atomic_semantic_patterns, "atomic_semantic_pattern")
            atomic_semantic_pattern.attrib["uri"] = record.get("URI")

            atomic_semantic_pattern_name = ET.SubElement(atomic_semantic_pattern, "atomic_semantic_pattern_name")
            atomic_semantic_pattern_name.attrib["uri"] = "http://vocab.getty.edu/aat/300456628"
            atomic_semantic_pattern_name_label = ET.SubElement(atomic_semantic_pattern_name, "atomic_semantic_pattern_name_label")
            atomic_semantic_pattern_name_label.text = record.get("UI_Name")

        composite_semantic_patterns = ET.SubElement(components, "composite_semantic_patterns")

        composite_patterns = self._airtable.get_all_records_from_table("Model")
        composite_patterns.extend(self._airtable.get_all_records_from_table("Collection"))

        for pattern in composite_patterns:
            record = pattern.get("fields")
            composite_semantic_pattern = ET.SubElement(composite_semantic_patterns, "composite_semantic_pattern")
            composite_semantic_pattern.attrib["uri"] = record.get("URI")

            composite_semantic_pattern_name = ET.SubElement(composite_semantic_pattern, "composite_semantic_pattern_name")
            composite_semantic_pattern_name.attrib["uri"] = "http://vocab.getty.edu/aat/300456628"
            composite_semantic_pattern_name_label = ET.SubElement(composite_semantic_pattern_name, "composite_semantic_pattern_name_label")
            composite_semantic_pattern_name_label.text = record.get("UI_Name")

        provenance = ET.SubElement(root, "provenance")
        version_data = ET.SubElement(provenance, "version_data")
        creation_data = ET.SubElement(provenance, "creation_data")
        if fields.get('Version'):
            version_number = ET.SubElement(version_data, "version_number")
            version_number.attrib["uri"] = "http://vocab.getty.edu/aat/300456598"
            version_number_label = ET.SubElement(version_number, "version_number_label")
            version_number_label.text = fields.get('Version')
        if fields.get('Version_Date'):
            version_publication_date = ET.SubElement(version_data, "version_publication_date")
            version_publication_date.attrib["uri"] = "http://vocab.getty.edu/aat/300456620"
            version_publication_date_label = ET.SubElement(version_publication_date, "version_publication_date_label")
            version_publication_date_label.text = fields.get('Version_Date')
        if fields.get('Last_Modified'):
            post_version_modification_date = ET.SubElement(version_data, "post_version_modification_date")
            post_version_modification_date.attrib["uri"] = "http://vocab.getty.edu/aat/300456620"
            post_version_modification_date_label = ET.SubElement(post_version_modification_date, "post_version_modification_date_label")
            post_version_modification_date_label.text = fields.get('Last_Modified')

        if fields.get("Author"):
            creators = ET.SubElement(creation_data, "creators")
            for record_id in fields.get("Author"):
                author = self._airtable.get_record_by_id('Actors', record_id)
                creator = ET.SubElement(creators, "creator")
                creator.attrib['uri'] = author.get("fields", {}).get("URI", '')

                creator_name = ET.SubElement(creator, "creator_name")
                creator_name.attrib["uri"] = "http://vocab.getty.edu/aat/300456619"
                creator_name_label = ET.SubElement(creator_name, "creator_name_label")
                creator_name_label.text = author.get("fields", {}).get("Name")

        funding = ET.SubElement(provenance, "funding")
        if fields.get("Funder"):
            funder = ET.SubElement(funding, "funder")

            for record_id in fields.get("Funder"):
                funder_record = self._airtable.get_record_by_id('Actors', record_id)

                funder_name = ET.SubElement(funder, "funder_name")
                funder_name.attrib["uri"] = "http://vocab.getty.edu/aat/300456619"
                funder_name_label = ET.SubElement(funder_name, "funder_name_label")
                funder_name_label.text = funder_record.get("fields", {}).get("Name")

        rough_string = ET.tostring(root, "utf-8")
        reparsed = minidom.parseString(rough_string)

        return reparsed.toprettyxml(indent=4 * " ")

