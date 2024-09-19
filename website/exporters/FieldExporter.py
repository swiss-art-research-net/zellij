from os import confstr
from typing import Union
from xml.dom import minidom

from pyairtable.api.types import RecordDict
from pyairtable.formulas import OR, EQUAL, STR_VALUE, match

from website.exporters.Exporter import Exporter

import xml.etree.ElementTree as ET


class FieldExporter(Exporter):
    def __init__(self):
        super().__init__()

    def _generate_xml(self) -> str:
        root = ET.Element("atomic_semantic_pattern")

        low_table = None
        for key, val in self.get_schema().items():
            if not isinstance(val, dict):
                continue

            if "GroupBy" in val:
                low_table = key
                break

        field = (self._airtable.get_record_by_formula(low_table, match({"ID": self._item}))
                 or self._airtable.get_record_by_id(low_table, self._item))

        if field is None:
            return ""

        base_field = self._airtable.get_record_by_id("Field", field.get("fields", {}).get("Field", [])[0])
        fields = base_field.get('fields')
        uri = ET.SubElement(root, "uri")
        uri.text = fields.get("URI")

        self._name = (fields.get("Identifer") or fields.get("Identifier", "")).replace(".", "_")

        definition = ET.SubElement(root, "definition")
        if 'Field' in self._prefill_data and self._prefill_data['Field']['exportable']:
            system_identifier = ET.SubElement(definition, "system_identifier")
            system_identifier_content = ET.SubElement(system_identifier, "identifier_content")
            system_identifier_content.text = field.get("fields").get("ID")
            system_identifier_type = ET.SubElement(system_identifier, "identifier_type")
            system_identifier_type_uri = ET.SubElement(system_identifier_type, "uri")
            system_identifier_type_uri.text = "http://vocab.getty.edu/aat/300404012"
            system_identifier_type_label = ET.SubElement(system_identifier_type, "label")
            system_identifier_type_label.text = "Unique Identifiers"

            identifiers = ET.SubElement(definition, "identifiers")
            identifier = ET.SubElement(identifiers, "identifier")
            identifier_content = ET.SubElement(identifier, "identifier_content")
            identifier_content.text = fields.get('ID')
            identifier_type = ET.SubElement(identifier, "identifier_type")
            identifier_type_uri = ET.SubElement(identifier_type, "uri")
            identifier_type_uri.text = "http://vocab.getty.edu/aat/300404012"
            identifier_type_label = ET.SubElement(identifier_type, "label")
            identifier_type_label.text = "Unique Identifiers"

            names = ET.SubElement(definition, "names")

            system_name = ET.SubElement(names, "system_name")
            system_name_content = ET.SubElement(system_name, "name_content")
            system_name_content.text = fields.get('System_Name')
            system_name_type = ET.SubElement(system_name, "name_type")
            system_name_type_uri = ET.SubElement(system_name_type, "uri")
            system_name_type_uri.text = "http://vocab.getty.edu/aat/300456630"
            system_name_type_label = ET.SubElement(system_name_type, "label")
            system_name_type_label.text = "System Name"

            for col in ["Field_UI_Name", "Field_UI_Name_Inverse", "Model_Specific_Field_Name"]:
                if field.get("fields").get(col) is None:
                    continue

                name = ET.SubElement(names, "name")
                name_content = ET.SubElement(name, "name_content")
                name_content.text = (
                    field.get("fields", {}).get(col, [])[0]
                    if isinstance(field.get("fields").get(col), list)
                    else field.get("fields").get(col)
                )
                name_type = ET.SubElement(name, "name_type")
                name_type_uri = ET.SubElement(name_type, "uri")
                name_type_label = ET.SubElement(name_type, "label")
                if col == "Field_UI_Name_Inverse":
                    name_type_uri.text = "http://vocab.getty.edu/aat/300456629"
                    name_type_label.text = "Inverse UI Name"
                else:
                    name_type_uri.text = "http://vocab.getty.edu/aat/300456628"
                    name_type_label.text = "UI Name"

                name_language = ET.SubElement(name, "name_language")
                name_language_uri = ET.SubElement(name_language, "uri")
                name_language_uri.text = "http://vocab.getty.edu/aat/300388277"
                name_language_label = ET.SubElement(name_language, "label")
                name_language_label.text = "English"

            descriptions = ET.SubElement(definition, "descriptions")
            description = ET.SubElement(descriptions, "description")

            description_content = ET.SubElement(description, "description_content")
            description_content.text = field.get("fields").get('Model_Specific_Description') or field.get(
                "fields").get('Description')

            description_type = ET.SubElement(description, "description_type")
            description_type_uri = ET.SubElement(description_type, "uri")
            description_type_uri.text = "http://vocab.getty.edu/aat/300456631"
            description_type_label = ET.SubElement(description_type, "label")
            description_type_label.text = 'Scope Note'

            description_language = ET.SubElement(description, "description_language")
            description_language_uri = ET.SubElement(description_language, "uri")
            description_language_uri.text = "http://vocab.getty.edu/aat/300388277"
            description_language_label = ET.SubElement(description_language, "label")
            description_language_label.text = 'English'

            ontological_scopes = ET.SubElement(definition, "ontological_scopes")
            ontological_scopes_records = fields.get('Ontology_Scope', []) if isinstance(fields.get('Ontology_Scope', []), list) else [fields.get('Ontology_Scope', [])]
            for ontology_id in ontological_scopes_records:
                record: Union[RecordDict, None] = None

                try:
                    record = self._airtable.get_record_by_id('CRM Class', ontology_id)
                except:
                    pass

                try:
                    if record is None:
                        record = self._airtable.get_record_by_id('Ontology_Class', ontology_id)
                except:
                    continue

                ontology_class = ET.SubElement(ontological_scopes, "ontology_class")
                ontology_class_uri = ET.SubElement(ontology_class, "uri")
                ontology_class_uri.text = record.get("fields", {}).get("Subject", "")

                ontology_label = ET.SubElement(ontology_class, "label")
                ontology_label.text = record.get("fields", {}).get("ID")

            semantic_path = ET.SubElement(definition, "semantic_path")
            semantic_path.text = fields.get('Ontological_Path')

            semantic_path_total = ET.SubElement(definition, "semantic_path_total")
            semantic_path_total.text = (
                    fields.get("Total_Ontological_Path") or
                    field.get("fields", {}).get('Total_Ontological_Path') or
                    field.get("fields", {}).get('Model_Fields_Total_Ontological_Path')
            )

            expected_data = ET.SubElement(definition, "expected_data")
            data_type = ET.SubElement(expected_data, "data_type")
            data_type_uri = ET.SubElement(data_type, "uri")
            data_type_label = ET.SubElement(data_type, "label")
            expected_value_type = fields.get("Expected_Value_Type", "")
            data_type_label.text = expected_value_type
            data_type_uri.text = self.value_types_terms.get(expected_value_type.lower())

            for set in fields.get('Expected_ConceptSet', []):
                field_control_set = self._airtable.get_record_by_id("ConceptSet", set)

                control_set = ET.SubElement(expected_data, "control_set")
                control_set_uri = ET.SubElement(control_set, "uri")
                control_set_uri.text = field_control_set.get("fields", {}).get("Name")
                ET.SubElement(control_set, "label")

            reference_control = ET.SubElement(expected_data, "reference_control")
            reference_control.text = fields.get("Set_Value")

            if fields.get("Expected_Value_Type") == "Collection":
                reference_patterns = ET.SubElement(expected_data, "reference_patterns")
                collections = fields.get('Expected_Collection_Model', [])

                for collection_field in self.get_records(collections, 'Collection'):
                    reference_pattern = ET.SubElement(reference_patterns, "reference_pattern")

                    reference_pattern_uri = ET.SubElement(reference_pattern, "uri")
                    reference_pattern_uri.text = collection_field.get("fields", {}).get("URI", "")

                    reference_pattern_label = ET.SubElement(reference_pattern, "label")
                    reference_pattern_label.text = collection_field.get("fields", {}).get("UI_Name")

                    reference_pattern_type = ET.SubElement(reference_pattern, "reference_pattern_type")
                    reference_pattern_type_uri = ET.SubElement(reference_pattern_type, "uri")
                    reference_pattern_type_uri.text = "http://vocab.getty.edu/aat/300456626"
                    reference_pattern_type_label = ET.SubElement(reference_pattern_type, "label")
                    reference_pattern_type_label.text = "Reference Collection"

                reference_pattern_type = ET.SubElement(reference_patterns, "reference_pattern_type")
                reference_pattern_type.text = "Collection"
            elif fields.get("Expected_Value_Type") == "Reference Model":
                reference_patterns = ET.SubElement(expected_data, "reference_patterns")
                models = fields.get('Expected_Resource_Model', [])

                for model_field in self.get_records(models, 'Model'):
                    reference_pattern = ET.SubElement(reference_patterns, "reference_pattern")

                    reference_pattern_uri = ET.SubElement(reference_pattern, "uri")
                    reference_pattern_uri.text = model_field.get("fields", {}).get("URI", "")

                    reference_pattern_label = ET.SubElement(reference_pattern, "label")
                    reference_pattern_label.text = model_field.get("fields", {}).get("UI_Name")

                    reference_pattern_type = ET.SubElement(reference_patterns, "reference_pattern_type")
                    reference_pattern_type_uri = ET.SubElement(reference_pattern_type, "uri")
                    reference_pattern_type_uri.text = "http://vocab.getty.edu/aat/300456625"
                    reference_pattern_type_label = ET.SubElement(reference_pattern_type, "label")
                    reference_pattern_type_label.text = "Reference Model"

                reference_pattern_type = ET.SubElement(reference_patterns, "reference_pattern_type")
                reference_pattern_type.text = "Model"

            pattern_context = ET.SubElement(root, "pattern_context")

            project_records = fields.get('Project', [])
            for project_field in self.get_records(project_records, 'Project'):
                semantic_pattern_space = ET.SubElement(pattern_context, "semantic_pattern_space")

                semantic_pattern_space_uri = ET.SubElement(semantic_pattern_space, "uri")
                semantic_pattern_space_uri.text = project_field.get("fields", {}).get("Namespace")

                semantic_pattern_space_label = ET.SubElement(semantic_pattern_space, "label")
                semantic_pattern_space_label.text = project_field.get("fields", {}).get("ID")

            composite_semantic_patterns_deployed_in = ET.SubElement(pattern_context,
                                                                    "composite_semantic_patterns_deployed_in")
            for model_field in self._airtable.get_multiple_records_by_formula('Model', OR(*list(
                    map(lambda x: EQUAL(STR_VALUE(x), 'RECORD_ID()'), fields.get("Model_Deployed", []))))):
                composite_semantic_pattern = ET.SubElement(composite_semantic_patterns_deployed_in,
                                                           "composite_semantic_pattern")
                composite_semantic_pattern_uri = ET.SubElement(composite_semantic_pattern, "uri")
                composite_semantic_pattern_uri.text = model_field.get("fields", {}).get("URI", "")

                composite_semantic_pattern_label = ET.SubElement(composite_semantic_pattern, "label")
                composite_semantic_pattern_label.text = model_field.get("fields", {}).get("UI_Name")

                composite_semantic_pattern_type = ET.SubElement(composite_semantic_pattern,
                                                                "composite_semantic_pattern_type")
                composite_semantic_pattern_type_uri = ET.SubElement(composite_semantic_pattern_type, "uri")
                composite_semantic_pattern_type_uri.text = "http://vocab.getty.edu/aat/300456625"
                composite_semantic_pattern_type_label = ET.SubElement(composite_semantic_pattern_type, "label")
                composite_semantic_pattern_type_label.text = "Reference Model"

            for collection_field in self._airtable.get_multiple_records_by_formula('Collection', OR(*list(
                    map(lambda x: EQUAL(STR_VALUE(x), 'RECORD_ID()'), fields.get("Collection_Deployed", []))))):
                composite_semantic_pattern = ET.SubElement(composite_semantic_patterns_deployed_in,
                                                           "composite_semantic_pattern")
                composite_semantic_pattern_uri = ET.SubElement(composite_semantic_pattern, "uri")
                composite_semantic_pattern_uri.text = collection_field.get("fields", {}).get("URI", "")

                composite_semantic_pattern_label = ET.SubElement(composite_semantic_pattern, "label")
                composite_semantic_pattern_label.text = collection_field.get("fields", {}).get("UI_Name")

                composite_semantic_pattern_type = ET.SubElement(composite_semantic_pattern,
                                                                "composite_semantic_pattern_type")
                composite_semantic_pattern_type_uri = ET.SubElement(composite_semantic_pattern_type, "uri")
                composite_semantic_pattern_type_uri.text = "http://vocab.getty.edu/aat/300456626"
                composite_semantic_pattern_type_label = ET.SubElement(composite_semantic_pattern_type, "label")
                composite_semantic_pattern_type_label.text = "Collection Model"

            serialization = ET.SubElement(root, "serialization")
            encodings_el = ET.SubElement(serialization, "encodings")

            if x3ml := fields.get("x3ml"):
                encoding = ET.SubElement(encodings_el, "encoding")

                encoding_content = ET.SubElement(encoding, "encoding_content")
                encoding_content.text = x3ml

                encoding_type = ET.SubElement(encoding, "encoding_type")
                encoding_type_uri = ET.SubElement(encoding_type, "uri")
                encoding_type_uri.text = "http://vocab.getty.edu/aat/300266654"
                encoding_type_label = ET.SubElement(encoding_type, "label")
                encoding_type_label.text = "x3ml"

                encoding_format = ET.SubElement(encoding, "encoding_format")
                encoding_format_uri = ET.SubElement(encoding_format, "uri")
                encoding_format_uri.text = "http://vocab.getty.edu/aat/300266654"
                encoding_type_label = ET.SubElement(encoding_format, "label")
                encoding_type_label.text = "xml"

            if sparql := fields.get("Total_SparQL"):
                encoding = ET.SubElement(encodings_el, "encoding")

                encoding_content = ET.SubElement(encoding, "encoding_content")
                encoding_content.text = sparql

                encoding_type = ET.SubElement(encoding, "encoding_type")
                encoding_type_uri = ET.SubElement(encoding_type, "uri")
                encoding_type_uri.text = "http://vocab.getty.edu/aat/300456634"
                encoding_type_label = ET.SubElement(encoding_type, "label")
                encoding_type_label.text = "rdf"

                encoding_format = ET.SubElement(encoding, "encoding_format")
                encoding_format_uri = ET.SubElement(encoding_format, "uri")
                encoding_format_uri.text = "http://vocab.getty.edu/aat/300456635"
                encoding_type_label = ET.SubElement(encoding_format, "label")
                encoding_type_label.text = "sparql"

            if turtle := fields.get("Total_Turtle"):
                encoding = ET.SubElement(encodings_el, "encoding")

                encoding_content = ET.SubElement(encoding, "encoding_content")
                encoding_content.text = turtle

                encoding_type = ET.SubElement(encoding, "encoding_type")
                encoding_type_uri = ET.SubElement(encoding_type, "uri")
                encoding_type_uri.text = "http://vocab.getty.edu/aat/300456634"
                encoding_type_label = ET.SubElement(encoding_type, "label")
                encoding_type_label.text = "rdf"

                encoding_format = ET.SubElement(encoding, "encoding_format")
                encoding_format_uri = ET.SubElement(encoding_format, "uri")
                encoding_format_uri.text = "http://vocab.getty.edu/aat/300456635"
                encoding_type_label = ET.SubElement(encoding_format, "label")
                encoding_type_label.text = "turtle"

            provenance = ET.SubElement(root, "provenance")
            version_data = ET.SubElement(provenance, "version_data")

            version_number = ET.SubElement(version_data, "version_number")
            version_number_content = ET.SubElement(version_number, "version_content")
            version_number_content.text = str(fields.get("Version"))
            version_number_type = ET.SubElement(version_number, "version_type")
            version_number_type_uri = ET.SubElement(version_number_type, "uri")
            version_number_type_uri.text = "http://vocab.getty.edu/aat/300456598"
            version_number_type_label = ET.SubElement(version_number_type, "label")
            version_number_type_label.text = "Version Numbers"

            version_publication_date = ET.SubElement(version_data, "version_publication_date")
            version_publication_date.text = fields.get("Version_Date")

            post_version_modification_date = ET.SubElement(version_data, "post_version_modification_date")
            post_version_modification_date.text = fields.get("Last_Modified")

            creation_data = ET.SubElement(provenance, "creation_data")
            creators = ET.SubElement(creation_data, "creators")
            for author_field in self._airtable.get_multiple_records_by_formula('Actors', OR(*list(
                    map(lambda x: EQUAL(STR_VALUE(x), 'RECORD_ID()'), fields.get("Author", []))))):
                creator = ET.SubElement(creators, "creator")

                creator_uri = ET.SubElement(creator, "uri")
                creator_uri.text = author_field.get("fields", {}).get("URI")
                creator_label = ET.SubElement(creator, "label")
                creator_label.text = author_field.get("fields", {}).get("Name")

            funding = ET.SubElement(provenance, "funding")
            funder_records = fields.get("Funder", [])
            if isinstance(funder_records, list):
                for funder_id in funder_records:
                    actor = self._airtable.get_record_by_id("Actors", funder_id)
                    funder = ET.SubElement(funding, "funder")

                    funder_uri = ET.SubElement(funder, "uri")
                    funder_uri.text = actor.get("fields", {}).get("URI")
                    funder_label = ET.SubElement(funder, "label")
                    funder_label.text = actor.get("fields", {}).get("Name")

            funding_project = ET.SubElement(funding, "funding_project")
            funding_project_records = fields.get("Project", [])
            if isinstance(funding_project_records, list):
                for project_id in funding_project_records:
                    project = self._airtable.get_record_by_id("Project", project_id)
                    project_field = ET.SubElement(funding_project, "project")

                    project_label = ET.SubElement(project_field, "label")
                    project_label.text = project.get("fields", {}).get("UI_Name")

                    project_uri = ET.SubElement(project_field, "uri")
                    project_uri.text = project.get("fields", {}).get("Namespace")

            semantic_context = ET.SubElement(definition, "semantic_context")
            ontologies = ET.SubElement(semantic_context, "ontologies")
            for ontology_field in self.get_records(fields.get("Ontology_Context", []), 'Ontology'):
                ontology = ET.SubElement(ontologies, "ontology")

                ontology_uri = ET.SubElement(ontology, "ontology_URI")
                ontology_uri.text = ontology_field.get("fields", {}).get("Namespace", "")

                ontology_prefix = ET.SubElement(ontology, "ontology_prefix")
                ontology_prefix.text = ontology_field.get("fields", {}).get("Prefix")

                ontology_name = ET.SubElement(ontology, "ontology_name")
                ontology_name.text = ontology_field.get("fields", {}).get("UI_Name")

                ontology_version = ET.SubElement(ontology, "ontology_version")
                ontology_version.text = ontology_field.get("fields", {}).get("Version")

        rough_string = ET.tostring(root, "utf-8")
        reparsed = minidom.parseString(rough_string)

        return reparsed.toprettyxml(indent=4 * " ")
