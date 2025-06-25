from datetime import date

from fpdf import Align
from fpdf.enums import TextEmphasis
from typing_extensions import override

from website.datasources import AirTableConnection
from website.exporters.PDFExporter import PDFExporter


class ProjectPDFExporter(PDFExporter):
    @override
    def load_data(self) -> dict:
        airtable = AirTableConnection.from_api_key(self.id)
        projects = airtable.get_all_records_from_table("Project")
        for project in projects:
            if project["fields"].get("ID"):
                self.data["project"] = project["fields"]
                break
        else:
            raise ValueError("No project found with a valid ID.")

        self.data["models"] = list(
            map(lambda x: x["fields"], airtable.get_all_records_from_table("Model"))
        )
        self.data["collections"] = list(
            map(
                lambda x: x["fields"], airtable.get_all_records_from_table("Collection")
            )
        )
        self.data["fields"] = list(
            map(lambda x: x["fields"], airtable.get_all_records_from_table("Field"))
        )
        self.data["ontologies"] = list(
            map(lambda x: x["fields"], airtable.get_all_records_from_table("Ontology"))
        )

        return {
            "name": self.data["project"].get("UI_Name", "Unknown Project"),
            "institution": self.data["project"].get("Author", "Unknown Author"),
            "version": self.data["project"].get("Version", "1.0"),
        }

    def _render_section(
        self, title: str, data: list[dict], configuration: dict
    ) -> None:
        self.section(title)
        self.div(title, align=Align.L, decoration=TextEmphasis.B)
        self.div(f"Total: {len(data)}", size=10)

        if not data:
            self.div("No models found.", align=Align.C)
            return

        rows = [tuple(configuration.values())]
        for data_row in data:
            rows.append(tuple([data_row.get(key, "") for key in configuration.keys()]))

        self.table(
            has_header=True,
            rows=tuple(rows),
        )

    def _metadata_section(self) -> None:
        self.h1(self.data["project"]["UI_Name"])
        self.div(date.today().strftime("%d/%m/%Y"), align=Align.C)
        self.div("Metadata", align=Align.C, decoration=TextEmphasis.B)
        self.table(
            has_header=False,
            rows=(
                ("KeyField", self.data["project"].get("ID", "")),
                ("Names", self.data["project"].get("UI_Name", "")),
                ("System Name", self.data["project"].get("System_Name", "")),
                ("Description", self.data["project"].get("Description", "")),
                ("Version", self.data["project"].get("Version", "")),
                ("Version Date", self.data["project"].get("Version_Date", "")),
                ("Last Modified", self.data["project"].get("Last_Modified", "")),
                ("Authors", self.data["project"].get("Author", "")),
                ("Funders", self.data["project"].get("Funder", "")),
                ("Funding Project", self.data["project"].get("Funding_Project", "")),
                ("Namespace", self.data["project"].get("Namespace", "")),
            ),
        )

    def _model_section(self) -> None:
        self._render_section(
            "Models",
            self.data.get("models", []),
            {
                "UI_Name": "Name",
                "Identifier": "ID",
                "Ontology_Scope": "Ontology Scope",
                "Description": "Description",
                "Version": "Version",
                "URI": "URI",
                "URL": "URL",
            },
        )

    def _collection_section(self) -> None:
        self._render_section(
            "Collections",
            self.data.get("collections", []),
            {
                "UI_Name": "Name",
                "Identifier": "ID",
                "Ontology_Scope": "Ontology Scope",
                "Description": "Description",
                "Version": "Version",
                "URI": "URI",
                "URL": "URL",
            },
        )

    def _field_section(self) -> None:
        self._render_section(
            "Fields",
            self.data.get("fields", []),
            {
                "UI_Name": "Name",
                "Identifier": "ID",
                "Ontology_Scope": "Ontology Scope",
                "Ontology_Long_Path": "Path",
                "Description": "Description",
                "Version": "Version",
                "URI": "URI",
            },
        )

    def _ontologies_section(self) -> None:
        self._render_section(
            "Ontologies",
            self.data.get("ontologies", []),
            {
                "UI_Name": "Name",
                "ID": "ID",
                "Description": "Description",
                "Version": "Version",
                "Namespace": "Namespace",
            },
        )

    @override
    def generate_content(self) -> None:
        self._metadata_section()
        self._model_section()
        self._collection_section()
        self._field_section()
        self._ontologies_section()
