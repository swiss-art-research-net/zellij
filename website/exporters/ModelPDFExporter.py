import os
from datetime import date

from fpdf import Align
from fpdf.enums import TextEmphasis
from pyairtable.formulas import EQUAL, OR, STR_VALUE, match
from typing_extensions import override

from website.datasources import AirTableConnection
from website.db import get_schema_from_api_key
from website.exporters.PDFExporter import PDFExporter

BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")


class ModelPDFExporter(PDFExporter):
    schemas = {}
    fields = {}

    def __init__(self, id: str, pattern: str, model_id: str):
        if not id:
            raise ValueError("ID is required for ModelPDFExporter.")

        self.pattern = pattern
        if not pattern:
            raise ValueError("Pattern is required for ModelPDFExporter.")

        if not model_id:
            raise ValueError("Model ID is required for ModelPDFExporter.")

        self.scraper = get_schema_from_api_key(pattern, id)

        if not self.scraper:
            raise ValueError(f"Invalid pattern or ID: {pattern}, {id}")

        super().__init__(id, model_id)

    @override
    def get_file_name(self) -> str:
        return f"{self.data['model'].get('UI_Name', 'Unknown Model')}.pdf"

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

        if self.model_id:
            self.data["model"] = self.airtable.get_record_by_formula(
                self.scraper["high_table"],
                OR(
                    EQUAL("RECORD_ID()", STR_VALUE(self.model_id)),
                    match({"ID": self.model_id}),
                ),
            )

        if self.data.get("model") is None:
            raise ValueError(f"No model found with ID: {self.model_id}")

        self.data["model"] = self.data.get("model", {})["fields"]

        return {
            "name": self.data["model"].get("UI_Name", "Unknown Model"),
            "institution": self.data["project"].get("Author", "Unknown Author"),
            "version": self.data["project"].get("Version", "1.0"),
        }

    def _metadata_section(self) -> None:
        self.h1(self.data["model"]["UI_Name"])
        self.div(self.data["project"]["UI_Name"], align=Align.C)
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
                ("KeyField", self.data["model"].get("ID", "")),
                ("Identifier", self.data["model"].get("Identifier", "")),
                ("Names", self.data["model"].get("UI_Name", "")),
                ("System Name", self.data["model"].get("System_Name", "")),
                ("Description", self.data["model"].get("Description", "")),
                ("Version", self.data["model"].get("Version", "")),
                ("Version Date", self.data["model"].get("Version_Date", "")),
                ("Last Modified", self.data["model"].get("Last_Modified", "")),
                ("Authors", author),
                ("Funders", funder),
                ("Funding Project", self.data["model"].get("Funding_Project", "")),
                ("Ontology", self.data["model"].get("Ontology_Scope_URI", "")),
                ("URI", self.data["model"].get("URI", "")),
                (
                    "URL",
                    f"{BASE_URL}/docs/list/{self.id}?scraper={self.pattern}&selectedMenuItem=%2Fdocs%2Fdisplay%2F{self.id}%2F{self.pattern}%3Fsearch%3D{self.model_id}",
                ),
            ),
        )

    @override
    def generate_content(self) -> None:
        self._metadata_section()
