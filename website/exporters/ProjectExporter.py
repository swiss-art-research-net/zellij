from xml.dom import minidom

from pyairtable.api.types import RecordDict

from website.exporters.Exporter import Exporter
import xml.etree.ElementTree as ET


class ProjectExporter(Exporter):
    def __init__(self):
        super().__init__()

    def _generate_xml(self) -> str:
        root = ET.Element("semantic_space")

        project: RecordDict = self._airtable.get_record_by_formula('Project', '{ID}')

        if project is None:
            return ""

        fields = project.get('fields', {})
        uri = ET.SubElement(root, "uri")
        uri.text = fields.get("Namespace")

        definition = ET.SubElement(root, "definition")

        names = ET.SubElement(definition, "names")

        system_name = ET.SubElement(names, "system_name")
        name_content = ET.SubElement(system_name, "name_content")
        name_content.text = fields.get('System_Name')
        name_type = ET.SubElement(system_name, "name_type")
        name_type_uri = ET.SubElement(name_type, "uri")
        name_type_uri.text = "http://vocab.getty.edu/aat/300456630"
        name_type_label = ET.SubElement(name_type, "label")
        name_type_label.text = 'System Name'

        name = ET.SubElement(names, "name")
        name_content = ET.SubElement(name, "name_content")
        name_content.text = fields.get('UI_Name')

        name_type = ET.SubElement(name, "name_type")
        name_type_uri = ET.SubElement(name_type, "uri")
        name_type_uri.text = "http://vocab.getty.edu/aat/300456628"
        name_type_label = ET.SubElement(name_type, "label")
        name_type_label.text = 'UI Name'
        name_language = ET.SubElement(name, "name_language")
        name_language_uri = ET.SubElement(name_language, "uri")
        name_language_uri.text = "http://vocab.getty.edu/aat/300388277"
        name_language_label = ET.SubElement(name_language, "label")
        name_language_label.text = 'English'
        self._name = uri.text

        identifiers = ET.SubElement(definition, "identifiers")
        system_identifier = ET.SubElement(identifiers, "system_identifier")
        identifier_content = ET.SubElement(system_identifier, "identifier_content")
        identifier_content.text = fields.get('ID')
        identifier_type = ET.SubElement(system_identifier, "identifier_type")
        identifier_type_uri = ET.SubElement(identifier_type, "uri")
        identifier_type_uri.text = "http://vocab.getty.edu/aat/300404012"
        identifier_type_label = ET.SubElement(identifier_type, "label")
        identifier_type_label.text = 'Unique Identifiers'

        descriptions = ET.SubElement(definition, "descriptions")
        description = ET.SubElement(descriptions, "description")

        description_content = ET.SubElement(description, "description_content")
        description_content.text = fields.get('Description')

        description_type = ET.SubElement(description, "description_type")
        description_type_uri = ET.SubElement(description_type, "uri")
        description_type_uri.text = "https://vocab.getty.edu/aat/300456631"
        description_type_label = ET.SubElement(description_type, "label")
        description_type_label.text = "Scope Note"

        description_language = ET.SubElement(description, "description_language")
        description_language_uri = ET.SubElement(description_language, "uri")
        description_language_uri.text = "http://vocab.getty.edu/aat/300388277"
        description_language_label = ET.SubElement(description_language, "label")
        description_language_label.text = 'English'

        namespace = ET.SubElement(definition, "namespace")
        namespace_content = ET.SubElement(namespace, "namespace_content")
        namespace_content.text = fields.get('Namespace')
        namespace_type = ET.SubElement(namespace, "namespace_type")
        namespace_type_uri = ET.SubElement(namespace_type, "uri")
        namespace_type_uri.text = "http://vocab.getty.edu/aat/300456599"
        namespace_type_label = ET.SubElement(namespace_type, "namespace_type_label")
        namespace_type_label.text = 'Namespace'

        semantic_context = ET.SubElement(root, "semantic_context")
        ontologies = ET.SubElement(semantic_context, "ontologies")
        for ontology in self._airtable.get_all_records_from_table('Ontology'):
            record = ontology.get("fields")
            ontology_el = ET.SubElement(ontologies, "ontology")

            ontology_prefix = ET.SubElement(ontology_el, "ontology_prefix")
            ontology_prefix.text = record.get("Prefix")

            ontology_name = ET.SubElement(ontology_el, "ontology_name")
            ontology_name.text = record.get("UI_Name")

            ontology_version = ET.SubElement(ontology_el, "ontology_version")
            ontology_version.text = record.get("Version")

            ontology_uri = ET.SubElement(ontology_el, "ontology_URI")
            ontology_uri.text = record.get("Namespace")

        dataspace_context = ET.SubElement(semantic_context, "dataspace_context")
        services = ET.SubElement(dataspace_context, "services")
        try:
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
        except:
            pass

        components = ET.SubElement(root, "components")
        atomic_semantic_patterns = ET.SubElement(components, "atomic_semantic_patterns")
        for field in self._airtable.get_all_records_from_table("Field"):
            record = field.get("fields")
            atomic_semantic_pattern = ET.SubElement(atomic_semantic_patterns, "atomic_semantic_pattern")

            atomic_semantic_pattern_uri = ET.SubElement(atomic_semantic_pattern, "uri")
            atomic_semantic_pattern_uri.text = record.get("URI")

            atomic_semantic_pattern_name = ET.SubElement(atomic_semantic_pattern, "label")
            atomic_semantic_pattern_name.text = record.get("UI_Name")

        composite_semantic_patterns = ET.SubElement(components, "composite_semantic_patterns")

        composite_patterns = list(map(lambda x: {**x, **{"type": "model"}}, self._airtable.get_all_records_from_table("Model")))
        composite_patterns.extend(list(map(lambda x: {**x, **{"type": "collection"}}, self._airtable.get_all_records_from_table("Collection"))))

        for pattern in composite_patterns:
            record = pattern.get("fields")
            composite_semantic_pattern = ET.SubElement(composite_semantic_patterns, "composite_semantic_pattern")

            composite_semantic_pattern_uri = ET.SubElement(composite_semantic_pattern, "uri")
            composite_semantic_pattern_uri.text = record.get("URI")

            composite_semantic_pattern_name = ET.SubElement(composite_semantic_pattern, "label")
            composite_semantic_pattern_name.text = record.get("UI_Name")

            composite_semantic_pattern_type = ET.SubElement(composite_semantic_pattern, "composite_semantic_pattern_type")
            composite_semantic_pattern_type_uri = ET.SubElement(composite_semantic_pattern_type, "uri")
            composite_semantic_pattern_type_label = ET.SubElement(composite_semantic_pattern_type, "label")

            if pattern["type"] == "model":
                composite_semantic_pattern_type_uri.text = "http://vocab.getty.edu/aat/300456625"
                composite_semantic_pattern_type_label.text = "Reference Model"
            else:
                composite_semantic_pattern_type_uri.text = "http://vocab.getty.edu/aat/300456626"
                composite_semantic_pattern_type_label.text = "Reference Collection"

        provenance = ET.SubElement(root, "provenance")
        version_data = ET.SubElement(provenance, "version_data")
        creation_data = ET.SubElement(provenance, "creation_data")
        if fields.get('Version'):
            version_number = ET.SubElement(version_data, "version_number")
            version_number_content = ET.SubElement(version_number, "version_number_content")
            version_number_content.text = fields.get('Version')
            version_type = ET.SubElement(version_number, "version_type")
            version_type_uri = ET.SubElement(version_type, "uri")
            version_type_uri.text = "http://vocab.getty.edu/aat/300456598"
            version_type_label = ET.SubElement(version_type, "label")
            version_type_label.text = 'Version Number'
        if fields.get('Version_Date'):
            version_publication_date = ET.SubElement(version_data, "version_publication_date")
            version_publication_date.text = fields.get('Version_Date')
        if fields.get('Last_Modified'):
            post_version_modification_date = ET.SubElement(version_data, "post_version_modification_date")
            post_version_modification_date.text = fields.get('Last_Modified')

        if fields.get("Author"):
            creators = ET.SubElement(creation_data, "creators")
            records = self.get_records(fields.get("Author", []), "Actors")
            for author in records:
                creator = ET.SubElement(creators, "creator")
                creator_name = ET.SubElement(creator, "label")
                creator_name.text = author.get("fields", {}).get("Name")

                if creator_name.text is None:
                    creator_name.text = author.get("fields", {}).get("ID")

                creator_uri = ET.SubElement(creator, "uri")
                creator_uri.text = author.get("fields", {}).get("URI")

        funding = ET.SubElement(provenance, "funding")
        if fields.get("Funder"):
            funder = ET.SubElement(funding, "funder")

            records = self.get_records(fields.get("Funder", []), "Actors")
            for funder_record in records:
                funder_name = ET.SubElement(funder, "label")
                funder_name.text = funder_record.get("fields", {}).get("Name")

                if funder_name.text is None:
                    funder_name.text = funder_record.get("fields", {}).get("ID")

                funder_uri = ET.SubElement(funder, "uri")
                funder_uri.text = funder_record.get("fields", {}).get("URI")

        rough_string = ET.tostring(root, "utf-8")
        reparsed = minidom.parseString(rough_string)

        return reparsed.toprettyxml(indent=4 * " ")

