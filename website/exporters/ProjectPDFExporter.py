import os
from datetime import date

from fpdf import Align
from fpdf.enums import TextEmphasis
from typing_extensions import override

from website.datasources import AirTableConnection
from website.exporters.PDFExporter import PDFExporter

BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")


class ProjectPDFExporter(PDFExporter):
    schemas = {}
    fields = {}

    @override
    def load_data(self) -> dict:
        self.airtable = AirTableConnection.from_api_key(self.id)
        projects = self.airtable.get_all_records_from_table("Project")
        for project in projects:
            if project["fields"].get("ID"):
                self.data["project"] = project["fields"]
                break
        else:
            raise ValueError("No project found with a valid ID.")

        self.data["models"] = list(
            map(
                lambda x: x["fields"], self.airtable.get_all_records_from_table("Model")
            )
        )
        self.data["collections"] = list(
            map(
                lambda x: x["fields"],
                self.airtable.get_all_records_from_table("Collection"),
            )
        )
        self.data["fields"] = list(
            map(
                lambda x: x["fields"], self.airtable.get_all_records_from_table("Field")
            )
        )
        self.data["ontologies"] = list(
            map(
                lambda x: x["fields"],
                self.airtable.get_all_records_from_table("Ontology"),
            )
        )

        return {
            "name": self.data["project"].get("UI_Name", "Unknown Project"),
            "institution": self.data["project"].get("Author", "Unknown Author"),
            "version": self.data["project"].get("Version", "1.0"),
        }

    def _find_table_name(self, title: str) -> str:
        """Find the table name based on the title."""
        if title == "Models":
            return "Model"
        elif title == "Collections":
            return "Collection"
        elif title == "Fields":
            return "Field"
        elif title == "Ontologies":
            return "Ontology"
        else:
            raise ValueError(f"Unknown section title: {title}")

    def _render_section(
        self,
        title: str,
        data: list[dict],
        configuration: dict,
        sizing: tuple[int, ...] | None,
    ) -> None:
        self.section(title)
        self.div(title, align=Align.L, decoration=TextEmphasis.B)
        self.div(f"Total: {len(data)}", size=10)
        table_name = self._find_table_name(title)

        if not data:
            self.div("No models found.", align=Align.C)
            return

        rows = [tuple(configuration.values())]
        for data_row in data:
            row = []

            for key in configuration.keys():
                if key == "URL":
                    scraper = "Model" if title == "Models" else "Collection"
                    row.append(
                        f"{BASE_URL}/docs/list/{self.id}?scraper={scraper}&selectedMenuItem=%2Fdocs%2Fdisplay%2F{self.id}%2F{scraper}%3Fsearch%3D{data_row.get('ID')}"
                    )
                else:
                    data_cell = data_row.get(key, "")

                    # handle references
                    if isinstance(data_cell, str):
                        if data_cell.startswith("rec"):
                            if table_name not in self.schemas:
                                self.schemas[table_name] = self.airtable.airtable.table(
                                    base_id=self.id, table_name=table_name
                                ).schema()
                            schema = self.schemas[table_name]

                            for f in schema.fields:
                                if f.name == key and f.type == "multipleRecordLinks":
                                    if data_cell not in self.fields:
                                        self.fields[data_cell] = (
                                            self.airtable.airtable.table(
                                                base_id=self.id,
                                                table_name=f.options.linked_table_id,
                                            ).get(data_cell)
                                        )

                                    data_cell = self.fields[data_cell]
                    elif isinstance(data_cell, list):
                        if len(data_cell) > 0 and data_cell[0].startswith("rec"):
                            new_data_cell = []
                            for rec in data_cell:
                                if table_name not in self.schemas:
                                    self.schemas[table_name] = (
                                        self.airtable.airtable.table(
                                            base_id=self.id, table_name=table_name
                                        ).schema()
                                    )
                                schema = self.schemas[table_name]

                                for f in schema.fields:
                                    if (
                                        f.name == key
                                        and f.type == "multipleRecordLinks"
                                    ):
                                        if rec not in self.fields:
                                            self.fields[rec] = (
                                                self.airtable.airtable.table(
                                                    base_id=self.id,
                                                    table_name=f.options.linked_table_id,
                                                ).get(rec)
                                            )

                                        new_data_cell.append(self.fields[rec])

                            data_cell = ", ".join(
                                map(
                                    lambda x: x.get("fields", {}).get("ID", ""),
                                    new_data_cell,
                                )
                            )

                    row.append(data_cell)

            rows.append(tuple(row))

        self.table(
            has_header=True,
            rows=tuple(rows),
            sizing=sizing,
        )

    def _metadata_section(self) -> None:
        self.h1(self.data["project"]["UI_Name"])
        self.div(date.today().strftime("%d/%m/%Y"), align=Align.C)
        self.div("Metadata", align=Align.C, decoration=TextEmphasis.B)

        author = self.data["project"].get("Author")
        if author and len(author) > 0 and author[0].startswith("rec"):
            author = (
                self.airtable.get_record_by_id(
                    "Actors", self.data["project"]["Author"][0]
                )
                .get("fields")
                .get("Name")
            )

        funder = self.data["project"].get("Funder")
        if funder and len(funder) > 0 and funder[0].startswith("rec"):
            funder = (
                self.airtable.get_record_by_id(
                    "Actors", self.data["project"]["Funder"][0]
                )
                .get("fields")
                .get("Name")
            )

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
                ("Authors", author),
                ("Funders", funder),
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
            (15, 15, 15, 30, 5, 10, 10),
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
            (15, 15, 15, 30, 5, 10, 10),
        )

    def _field_section(self) -> None:
        self._render_section(
            "Fields",
            self.data.get("fields", []),
            {
                "UI_Name": "Name",
                "Identifer": "ID",
                "Ontology_Scope": "Ontology Scope",
                "Ontology_Long_Path": "Path",
                "Description": "Description",
                "Version": "Version",
                "URI": "URI",
            },
            (15, 15, 15, 20, 25, 5, 5),
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
            None,
        )

    @override
    def generate_content(self) -> None:
        self._metadata_section()
        self._model_section()
        self._collection_section()
        self._field_section()
        self._ontologies_section()
