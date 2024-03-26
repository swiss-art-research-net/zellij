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

    def __init__(self):
        super().__init__()

    def _generate_xml(self) -> str:
        root = ET.Element("composite_semantic_pattern")
        names = ET.SubElement(root, "names")
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
                    id = ET.SubElement(root, "system_identifier")
                    id.text = val

                if self._prefill_group.get(key, {}).get('name') == "Description":
                    descriptions = ET.SubElement(root, "descriptions")
                    description = ET.SubElement(descriptions, "description")
                    description_content = ET.SubElement(description, "description_content")
                    description_content.text = val

                    description_type = ET.SubElement(description, "description_type")
                    description_type.text = "Scope Note"

                    description_language = ET.SubElement(description, "description_language")
                    description_language.text = "English"

                    description_preference = ET.SubElement(description, "description_preference")
                    description_preference.text = "Preferred"


        rough_string = ET.tostring(root, "utf-8")
        reparsed = minidom.parseString(rough_string)

        return reparsed.toprettyxml(indent=4 * " ")

    def initialize(self, selected_scheme: str, api_key: str, item: str):
        schemas, secretkey = generate_airtable_schema(api_key)
        airtable = AirTableConnection(decrypt(secretkey), api_key)

        schema = schemas[selected_scheme]
        _, prefill_group, _ = get_prefill(api_key, schema.get("id"))

        self._results = airtable.getListOfGroups(schema)
        self._prefill_group = prefill_group
        self._item = item

    def export(self) -> io.BytesIO:
        file = io.BytesIO()

        content = self._generate_xml()

        file.write(content.encode('utf-8'))
        file.seek(0)

        return file
