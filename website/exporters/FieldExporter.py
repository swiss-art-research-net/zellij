from xml.dom import minidom

from exporters.Exporter import Exporter

import xml.etree.ElementTree as ET


class FieldExporter(Exporter):
    def __init__(self):
        super().__init__()

    def _generate_xml(self) -> str:
        root = ET.Element("atomic_semantic_pattern")
        definition = ET.SubElement(root, "definition")

        for result in self._results:
            if result.get("KeyField") != self._item:
                continue

            field = self._airtable.get_record_by_id("Field", result.get("Field"))

        rough_string = ET.tostring(root, "utf-8")
        reparsed = minidom.parseString(rough_string)

        return reparsed.toprettyxml(indent=4 * " ")
