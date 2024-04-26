from xml.dom import minidom

from exporters.Exporter import Exporter

import xml.etree.ElementTree as ET


class FieldExporter(Exporter):
    def __init__(self):
        super().__init__()

    def _generate_xml(self) -> str:
        root = ET.Element("atomic_semantic_pattern")
        definition = ET.SubElement(root, "definition")

        field = self._airtable.get_record_by_id("Field", self._item)
        fields = field.get('fields')

        if 'Field' in self._prefill_data and self._prefill_data['Field']['exportable']:
            system_identifier = ET.SubElement(definition, "system_identifier")
            system_identifier.text = fields.get('Identifer')

            descriptions = ET.SubElement(definition, "descriptions")
            description = ET.SubElement(descriptions, "description")
            description_content = ET.SubElement(description, "description_content")
            description_content.text = fields.get('Description')

            description_type = ET.SubElement(description, "description_type")
            description_type.text = 'Scope Note'

            description_language = ET.SubElement(description, "description_language")
            description_language.text = 'English'

            description_preference = ET.SubElement(description, "description_preference")
            description_preference.text = 'Preferred'

            ontology_scope = ET.SubElement(definition, "ontology_scope")
            for ontology_id in fields.get('Ontology_Scope'):
                record = self._airtable.get_record_by_id('CRM Class', ontology_id)
                ontology_class = ET.SubElement(ontology_scope, "ontology_class")

                class_name = ET.SubElement(ontology_class, "class_name")
                class_name.text = record.get("fields", {}).get("ID")

                class_uri = ET.SubElement(ontology_class, "class_URI")
                class_uri.text = record.get("fields", {}).get("Subject")

            semantic_path = ET.SubElement(definition, "semantic_path")
            semantic_path.text = fields.get('Ontological_Path')

            semantic_path_total = ET.SubElement(definition, "semantic_path_total")
            semantic_path_total.text = fields.get('Total_Ontological_Path')

            expected_data = ET.SubElement(definition, "expected_data")
            data_type = ET.SubElement(expected_data, "data_type")
            data_type.text = fields.get("Expected_Value_Type")

            control_set = ET.SubElement(expected_data, "control_set")
            control_set.text = fields.get("Expected_ConceptSet")

            reference_control = ET.SubElement(expected_data, "reference_control")
            reference_control.text = fields.get("Set_Value")

            if fields.get("Expected_Value_Type") == "Collection":
                reference_patterns = ET.SubElement(expected_data, "reference_patterns")
                reference_pattern = ET.SubElement(reference_patterns, "reference_pattern")
                reference_pattern.text = "Expected Collection Model"


                collection = fields.get('Expected_Collection_Model')[0] if fields.get('Expected_Collection_Model') else None
                if collection:
                    collection_field = self._airtable.get_record_by_id('Collection', collection)

                    reference_pattern_name = ET.SubElement(reference_patterns, "reference_pattern_name")
                    reference_pattern_name.text = collection_field.get("fields", {}).get("UI_Name")

                reference_pattern_type = ET.SubElement(reference_patterns, "reference_pattern_type")
                reference_pattern_type.text = "Collection"
            elif fields.get("Expected_Value_Type") == "Reference Model":
                reference_patterns = ET.SubElement(expected_data, "reference_patterns")
                reference_pattern = ET.SubElement(reference_patterns, "reference_pattern")
                reference_pattern.text = "Expected Resource Model"

                model = fields.get('Expected_Resource_Model')[0] if fields.get('Expected_Resource_Model') else None

                if model:
                    model_field = self._airtable.get_record_by_id('Model', model)

                    reference_pattern_name = ET.SubElement(reference_patterns, "reference_pattern_name")
                    reference_pattern_name.text = model_field.get("fields", {}).get("UI_Name")

                reference_pattern_type = ET.SubElement(reference_patterns, "reference_pattern_type")
                reference_pattern_type.text = "Model"

            pattern_context = ET.SubElement(root, "pattern_context")
            composite_semantic_patterns_deployed_in = ET.SubElement(pattern_context, "composite_semantic_patterns_deployed_in")
            for model in fields.get("Model_Deployed", []):
                model_field = self._airtable.get_record_by_id('Model', model)

                composite_semantic_pattern = ET.SubElement(composite_semantic_patterns_deployed_in, "composite_semantic_pattern")
                composite_semantic_pattern_name = ET.SubElement(composite_semantic_pattern, "composite_semantic_pattern_name")
                composite_semantic_pattern_name.text = model_field.get("fields", {}).get("UI_Name")

                composite_semantic_pattern_uri = ET.SubElement(composite_semantic_pattern, "composite_semantic_pattern_URI")
                composite_semantic_pattern_uri.text = model_field.get("fields", {}).get("URI")

                composite_semantic_pattern_type = ET.SubElement(composite_semantic_pattern, "composite_semantic_pattern_type")
                composite_semantic_pattern_type.text = "Model"
            for collection in fields.get("Collection_Deployed", []):
                collection_field = self._airtable.get_record_by_id('Collection', collection)

                composite_semantic_pattern = ET.SubElement(composite_semantic_patterns_deployed_in, "composite_semantic_pattern")
                composite_semantic_pattern_name = ET.SubElement(composite_semantic_pattern, "composite_semantic_pattern_name")
                composite_semantic_pattern_name.text = collection_field.get("fields", {}).get("UI_Name")

                composite_semantic_pattern_uri = ET.SubElement(composite_semantic_pattern, "composite_semantic_pattern_URI")
                composite_semantic_pattern_uri.text = collection_field.get("fields", {}).get("URI")

                composite_semantic_pattern_type = ET.SubElement(composite_semantic_pattern, "composite_semantic_pattern_type")
                composite_semantic_pattern_type.text = "Collection"

            serialization = ET.SubElement(root, "serialization")
            encodings_el = ET.SubElement(serialization, "encodings")

            if x3ml := fields.get("x3ml"):
                encoding = ET.SubElement(encodings_el, "encoding")

                encoding_content = ET.SubElement(encoding, "encoding_content")
                encoding_content.text = x3ml

                encoding_type = ET.SubElement(encoding, "encoding_type")
                encoding_type.text = "x3ml"

                encoding_format = ET.SubElement(encoding, "encoding_format")
                encoding_format.text = "xml"

            if sparql := fields.get("Total_SparQL"):
                encoding = ET.SubElement(encodings_el, "encoding")

                encoding_content = ET.SubElement(encoding, "encoding_content")
                encoding_content.text = sparql

                encoding_type = ET.SubElement(encoding, "encoding_type")
                encoding_type.text = "SparQL"

                encoding_format = ET.SubElement(encoding, "encoding_format")
                encoding_format.text = "sparql"

            provenance = ET.SubElement(root, "provenance")
            version_data = ET.SubElement(provenance, "version_data")

            version_number = ET.SubElement(version_data, "version_number")
            version_number.text = str(fields.get("Version"))

            version_publication_date = ET.SubElement(version_data, "version_publication_date")
            version_publication_date.text = fields.get("Version_Date")

            post_version_modification_date = ET.SubElement(version_data, "post_version_modification_date")
            post_version_modification_date.text = fields.get("Last_Modified")

        rough_string = ET.tostring(root, "utf-8")
        reparsed = minidom.parseString(rough_string)

        return reparsed.toprettyxml(indent=4 * " ")
