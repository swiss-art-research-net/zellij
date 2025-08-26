import xml.etree.ElementTree as ET
from xml.dom import minidom

from pyairtable.formulas import EQ, OR, match, quoted

from website.exporters.Exporter import Exporter


class ModelExporter(Exporter):
    def __init__(self):
        super().__init__()

    def _generate_xml(self) -> str:
        root = ET.Element("composite_semantic_pattern")
        uri = ET.SubElement(root, "uri")
        definition = ET.SubElement(root, "definition")
        semantic_context = ET.SubElement(root, "semantic_context")
        pattern_context = ET.SubElement(root, "pattern_context")

        type_el = ET.SubElement(definition, "type")
        type_uri = ET.SubElement(type_el, "uri")
        type_label = ET.SubElement(type_el, "label")

        if "Collection" in self.get_schema().keys():
            type_uri.text = "http://vocab.getty.edu/aat/300456626"
            type_label.text = "Collection Model"
        else:
            type_uri.text = "http://vocab.getty.edu/aat/300456625"
            type_label.text = "Reference Model"

        provenance = ET.SubElement(root, "provenance")
        version_data = ET.SubElement(provenance, "version_data")
        creation_data = ET.SubElement(provenance, "creation_data")
        funding = ET.SubElement(provenance, "funding")

        serialization = ET.SubElement(root, "serialization")
        encodings_el = ET.SubElement(serialization, "encodings")

        identifiers = ET.SubElement(definition, "identifiers")

        names = ET.SubElement(definition, "names")
        for result in self._results:
            if result.get("KeyField") != self._item:
                continue
            for key, val in result.items():
                if (
                    not self._prefill_group.get(key, {}).get("exportable", False)
                    and key != "KeyField"
                ):
                    continue

                if key == "URI":
                    uri.text = val

                if key == "Identifer" or key == "Identifier":
                    self._name = val.replace(".", "_")

                if self._prefill_group.get(key, {}).get("name") == "UI_Name":
                    name = ET.SubElement(names, "name")
                    name_content = ET.SubElement(name, "name_content")
                    name_content.text = val

                    name_type = ET.SubElement(name, "name_type")
                    name_type_uri = ET.SubElement(name_type, "uri")
                    name_type_uri.text = "http://vocab.getty.edu/aat/300456628"
                    name_type_label = ET.SubElement(name_type, "label")
                    name_type_label.text = "UI_Name"

                    name_language = ET.SubElement(name, "name_language")
                    name_language_uri = ET.SubElement(name_language, "uri")
                    name_language_uri.text = "http://vocab.getty.edu/aat/300388277"
                    name_language_label = ET.SubElement(name_language, "label")
                    name_language_label.text = "English"

                if self._prefill_group.get(key, {}).get("name") == "System_Name":
                    system_name = ET.SubElement(names, "system_name")
                    system_name_content = ET.SubElement(
                        system_name, "system_name_content"
                    )
                    system_name_content.text = val

                    system_name_type = ET.SubElement(system_name, "system_name_type")
                    system_name_type_uri = ET.SubElement(system_name_type, "uri")
                    system_name_type_uri.text = "http://vocab.getty.edu/aat/300456630"
                    system_name_type_label = ET.SubElement(system_name_type, "label")
                    system_name_type_label.text = "System Name"

                if self._prefill_group.get(key, {}).get("name") == "Identifier":
                    system_identifier = ET.SubElement(identifiers, "system_identifier")
                    system_identifier_content = ET.SubElement(
                        system_identifier, "system_identifier_content"
                    )
                    system_identifier_content.text = val
                    system_identifier_type = ET.SubElement(
                        system_identifier, "system_identifier_type"
                    )
                    system_identifier_type_uri = ET.SubElement(
                        system_identifier_type, "uri"
                    )
                    system_identifier_type_uri.text = (
                        "http://vocab.getty.edu/aat/300404012"
                    )
                    system_identifier_type_label = ET.SubElement(
                        system_identifier_type, "label"
                    )
                    system_identifier_type_label.text = "Unique Identifier"

                if self._prefill_group.get(key, {}).get("name") == "Ontology_Context":
                    ontologies = ET.SubElement(semantic_context, "ontologies")

                    for ontology_field in self.get_records(val, "Ontology"):
                        ontology = ET.SubElement(ontologies, "ontology")

                        ontology_uri = ET.SubElement(ontology, "ontology_URI")
                        ontology_uri.text = ontology_field.get("fields", {}).get(
                            "Namespace", ""
                        )

                        ontology_prefix = ET.SubElement(ontology, "ontology_prefix")
                        ontology_prefix.text = ontology_field.get("fields", {}).get(
                            "Prefix"
                        )

                        ontology_name = ET.SubElement(ontology, "ontology_name")
                        ontology_name.text = ontology_field.get("fields", {}).get(
                            "UI_Name"
                        )

                        ontology_version = ET.SubElement(ontology, "ontology_version")
                        ontology_version.text = ontology_field.get("fields", {}).get(
                            "Version"
                        )

                if key == "KeyField":
                    identifier = ET.SubElement(identifiers, "identifier")

                    identifier_content = ET.SubElement(identifier, "identifier_content")
                    identifier_content.text = val

                    identifier_type = ET.SubElement(identifier, "identifier_type")
                    identifier_type_uri = ET.SubElement(identifier_type, "uri")
                    identifier_type_uri.text = "http://vocab.getty.edu/aat/300404012"
                    identifier_type_label = ET.SubElement(identifier_type, "label")
                    identifier_type_label.text = "Unique Identifier"

                    if not self._name:
                        self._name = val

                if self._prefill_group.get(key, {}).get("name") == "Description":
                    descriptions = ET.SubElement(definition, "descriptions")
                    description = ET.SubElement(descriptions, "description")
                    description_content = ET.SubElement(
                        description, "description_content"
                    )
                    description_content.text = val

                    description_type = ET.SubElement(description, "description_type")
                    description_type_uri = ET.SubElement(description_type, "uri")
                    description_type_uri.text = "http://vocab.getty.edu/aat/300456631"
                    description_type_label = ET.SubElement(description_type, "label")
                    description_type_label.text = "Scope Note"

                    description_language = ET.SubElement(
                        description, "description_language"
                    )
                    description_language_uri = ET.SubElement(
                        description_language, "uri"
                    )
                    description_language_uri.text = (
                        "http://vocab.getty.edu/aat/300388277"
                    )
                    description_language_label = ET.SubElement(
                        description_language, "label"
                    )
                    description_language_label.text = "English"

                if self._prefill_group.get(key, {}).get("name") == "Model_NameSpaces":
                    semantic_context = ET.SubElement(root, "semantic_context")

                if self._prefill_group.get(key, {}).get("name") == "Project":
                    for record in self.get_records(val, "Project"):
                        semantic_pattern_space = ET.SubElement(
                            pattern_context, "semantic_pattern_space"
                        )
                        semantic_pattern_space_uri = ET.SubElement(
                            semantic_pattern_space, "uri"
                        )
                        semantic_pattern_space_uri.text = record.get("fields", {}).get(
                            "Namespace"
                        )

                        semantic_pattern_space_name = ET.SubElement(
                            semantic_pattern_space, "name"
                        )
                        semantic_pattern_space_name.text = record.get("fields", {}).get(
                            "UI_Name"
                        )

                if (
                    self._prefill_group.get(key, {}).get("name")
                    == "Fields_Expected_Resource_Model"
                    or self._prefill_group.get(key, {}).get("name")
                    == "Fields_Expected_Collection_Model"
                ):
                    semantic_patterns_deployed_in = ET.SubElement(
                        pattern_context, "semantic_patterns_deployed_in"
                    )
                    target_of = ET.SubElement(
                        semantic_patterns_deployed_in, "target_of"
                    )

                    for field in self._airtable.get_multiple_records_by_formula(
                        "Field",
                        OR(*list(map(lambda x: EQ(quoted(x), "RECORD_ID()"), val))),
                    ):
                        fields = field.get("fields")

                        atomic_semantic_pattern = ET.SubElement(
                            target_of, "atomic_semantic_pattern"
                        )
                        atomic_semantic_pattern_uri = ET.SubElement(
                            atomic_semantic_pattern, "uri"
                        )
                        atomic_semantic_pattern_uri.text = fields.get("URI")
                        atomic_semantic_pattern_label = ET.SubElement(
                            atomic_semantic_pattern, "label"
                        )
                        atomic_semantic_pattern_label.text = fields.get("UI_Name")

                if (
                    self._prefill_group.get(key, {}).get("name") == "Model_Fields"
                    or self._prefill_group.get(key, {}).get("name")
                    == "Collection_Fields"
                ):
                    referenced_table = self._prefill_group.get(key, {}).get("name")
                    composition = ET.SubElement(root, "composition")

                    fields_to_populate = {}
                    fields_uris = []

                    for model_field in self.get_records(val, referenced_table):
                        pattern = ET.SubElement(composition, "pattern")

                        field_ui_names = model_field.get("fields", {}).get(
                            "Field_UI_Name", ""
                        )
                        pattern_name = ET.SubElement(pattern, "pattern_name")
                        if isinstance(field_ui_names, list):
                            pattern_name.text = (
                                field_ui_names[0] if len(field_ui_names) > 0 else ""
                            )
                        else:
                            pattern_name.text = field_ui_names

                        fields = model_field.get("fields", {}).get("Field", [])
                        fields_key = fields[0] if isinstance(fields, list) else fields
                        if len(fields) > 0:
                            fields_to_populate[fields_key] = pattern
                            fields_uris.append(fields_key)

                    for field in self._airtable.get_multiple_records_by_formula(
                        "Field",
                        OR(
                            *list(
                                map(
                                    lambda x: OR(
                                        match({"ID": x}),
                                        EQ(quoted(x), "RECORD_ID()"),
                                    ),
                                    fields_uris,
                                )
                            )
                        ),
                    ):
                        if "rec" in fields_uris[0]:
                            pattern_uri = ET.SubElement(
                                fields_to_populate[field["id"]], "pattern_URI"
                            )
                        else:
                            pattern_uri = ET.SubElement(
                                fields_to_populate[field["fields"].get("ID")],
                                "pattern_URI",
                            )
                        pattern_uri.text = field.get("fields", {}).get("URI")

                if (
                    self._prefill_group.get(key, {}).get("name") == "Total_SparQL"
                    or self._prefill_group.get(key, {}).get("name") == "SparQL"
                    or self._prefill_group.get(key, {}).get("name") == "SparQL_Count"
                    or self._prefill_group.get(key, {}).get("name")
                    == "SparQL_Count_Total"
                ):
                    encoding = ET.SubElement(encodings_el, "encoding")

                    encoding_content = ET.SubElement(encoding, "encoding_content")
                    encoding_content.text = val

                    encoding_type = ET.SubElement(encoding, "encoding_type")
                    encoding_type.text = "rdf"

                    encoding_format = ET.SubElement(encoding, "encoding_format")
                    encoding_format.text = "sparql"

                if self._prefill_group.get(key, {}).get("name") == "x3ml":
                    encoding = ET.SubElement(encodings_el, "encoding")

                    encoding_content = ET.SubElement(encoding, "encoding_content")
                    encoding_content.text = val

                    encoding_type = ET.SubElement(encoding, "encoding_type")
                    encoding_type.attrib["uri"] = "http://vocab.getty.edu/aat/300266654"
                    encoding_type_label = ET.SubElement(
                        encoding_type, "encoding_type_label"
                    )
                    encoding_type_label.text = "x3ml"

                    encoding_format = ET.SubElement(encoding, "encoding_format")
                    encoding_format.attrib["uri"] = (
                        "http://vocab.getty.edu/aat/300266654"
                    )
                    encoding_type_label = ET.SubElement(
                        encoding_format, "encoding_format_label"
                    )
                    encoding_type_label.text = "xml"

                if self._prefill_group.get(key, {}).get("name") == "Version":
                    version_number = ET.SubElement(version_data, "version_number")
                    version_number_content = ET.SubElement(
                        version_number, "version_content"
                    )
                    version_number_content.text = val
                    version_number_type = ET.SubElement(version_number, "version_type")
                    version_number_type_uri = ET.SubElement(version_number_type, "uri")
                    version_number_type_uri.text = (
                        "http://vocab.getty.edu/aat/300456598"
                    )
                    version_number_type_label = ET.SubElement(
                        version_number_type, "label"
                    )
                    version_number_type_label.text = "Version Numbers"
                if self._prefill_group.get(key, {}).get("name") == "Version_Date":
                    version_publication_date = ET.SubElement(
                        version_data, "version_publication_date"
                    )
                    version_publication_date.text = val
                if self._prefill_group.get(key, {}).get("name") == "Last_Modified":
                    post_version_modification_date = ET.SubElement(
                        version_data, "post_version_modification_date"
                    )
                    post_version_modification_date.text = val

                if self._prefill_group.get(key, {}).get("name") == "Author":
                    creators = ET.SubElement(creation_data, "creators")
                    for author in self.get_records(val, "Actors"):
                        creator = ET.SubElement(creators, "creator")

                        creator_uri = ET.SubElement(creator, "uri")
                        creator_uri.text = author.get("fields", {}).get("URI")
                        creator_label = ET.SubElement(creator, "label")
                        creator_label.text = author.get("fields", {}).get("Name")
                        if not creator_label.text:
                            creator_label.text = author.get("fields", {}).get("ID")

                if self._prefill_group.get(key, {}).get("name") == "Funder":
                    try:
                        records = self.get_records(val, "Institution")
                    except Exception:
                        records = self.get_records(val, "Actors")

                    for funder_record in records:
                        funder = ET.SubElement(funding, "funder")
                        funder_uri = ET.SubElement(funder, "uri")
                        funder_uri.text = funder_record.get("fields", {}).get("URI")
                        funder_label = ET.SubElement(funder, "label")
                        funder_label.text = funder_record.get("fields", {}).get("Name")
                        if not funder_label.text:
                            funder_label.text = funder_record.get("fields", {}).get(
                                "ID"
                            )

                if self._prefill_group.get(key, {}).get("name") == "Funding_Project":
                    for project in self.get_records(val, "Project"):
                        funding_project = ET.SubElement(provenance, "funding_project")

                        project_label = ET.SubElement(funding_project, "label")
                        project_label.text = project.get("fields", {}).get("UI_Name")

                        project_uri = ET.SubElement(funding_project, "uri")
                        project_uri.text = project.get("fields", {}).get("Namespace")

        rough_string = ET.tostring(root, "utf-8")
        reparsed = minidom.parseString(rough_string)

        return reparsed.toprettyxml(indent=4 * " ")
