import math
import os
from datetime import date
from io import BytesIO

import mermaid as md
from fpdf import Align
from fpdf.enums import TextEmphasis
from pyairtable.formulas import EQUAL, OR, STR_VALUE, match
from typing_extensions import override

from website.datasources import AirTableConnection, get_prefill
from website.db import get_schema_from_api_key
from website.exporters.PDFExporter import PDFExporter
from website.functions import generate_ontology_graph
from ZellijData.TurtleCodeBlock import TurtleCodeBlock

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

        self.schema, self.scraper = get_schema_from_api_key(pattern, id)

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

        prefill_data, _, group_sort = get_prefill(self.id, self.schema["id"])

        if isinstance(prefill_data, str):
            raise ValueError(
                f"Invalid prefill data for model {self.model_id}: {prefill_data}"
            )

        self.data["item"] = self.airtable.getSingleGroupedItem(
            self.model_id,
            self.schema,
            prefill_data=prefill_data,
            group_sort=group_sort,
        )

        fields_to_group = [
            key for key, value in prefill_data.items() if value.get("groupable", False)
        ]
        if len(fields_to_group) > 0:
            self.airtable.groupFields(self.data["item"], fields_to_group[0], group_sort)
        else:
            self.airtable.groupFields(self.data["item"])

        self.hidden_keys = [
            key for key, value in prefill_data.items() if value.get("hideable", False)
        ]
        self.graph_field = next(
            (
                key
                for key, val in prefill_data.items()
                if val["function"] == "graph_display"
            ),
            None,
        )

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

    def _generate_fields_sub_section(self, title: str, data: dict) -> None:
        self.sub_section(f"{title}: Fields")
        self.div(f"{title}: Fields", align=Align.C, decoration=TextEmphasis.B)
        rows = []
        for entry in data:
            if len(rows) == 0:
                rows.append(tuple(entry.keys()))

            data_row = []
            for field_key, field in entry.items():
                if field_key in self.hidden_keys:
                    continue

                if isinstance(field, list):
                    if any([isinstance(f, dict) for f in field]):
                        field = ", ".join(
                            list(map(lambda x: x["fields"].get("UI_Name", ""), field))
                        )
                    else:
                        field = ", ".join(field)
                elif field is None:
                    field = ""
                elif not isinstance(field, str):
                    field = str(field)

                if len(field) > 100:
                    field = field[:97] + "..."
                data_row.append(field)
            rows.append(tuple(data_row))

        self.table(
            has_header=True,
            rows=tuple(rows),
        )

    def _generate_graph_sub_section(self, title: str, data: dict) -> None:
        self.sub_section(f"{title}: Ontology Graph")
        self.div(f"{title}: Ontology Graph", align=Align.C, decoration=TextEmphasis.B)
        text = ""
        for entry in data:
            for field_key, field in entry.items():
                if field_key != self.graph_field:
                    continue

                text += field

        TurtlePrefix = ""
        if "Turtle RDF" in self.data["item"].ExtraFields:
            turtle_prefix = TurtleCodeBlock(self.data["item"].ExtraFields["Turtle RDF"])
            TurtlePrefix = "\n".join(turtle_prefix.prefix)

        allturtle = TurtlePrefix + "\n\n" + text
        turtle = TurtleCodeBlock(allturtle)
        graph_text = turtle.text()
        mmd = md.Mermaid(
            generate_ontology_graph(graph_text),
            width=math.floor(self.pdf.w - 20) * 8,
            height=math.floor(self.pdf.h - 60) * 8,
        )
        self.pdf.image(
            BytesIO(mmd.img_response.content),
            x=10,
            y=20,
            w=self.pdf.w - 20,
            h=self.pdf.h - 60,
        )

    def _generate_rdf_sub_section(self, title: str, data: dict) -> None:
        self.sub_section(f"{title}: Data Sample")
        self.div(f"{title}: Data Sample", align=Align.C, decoration=TextEmphasis.B)

    def _generate_category_section(self, title: str, data: dict) -> None:
        self.section(title)
        self.div(title, align=Align.C, decoration=TextEmphasis.B)
        self._generate_fields_sub_section(title, data)
        if self.graph_field is not None:
            self.pdf.add_page()
            self._generate_graph_sub_section(title, data)
        self.pdf.add_page()
        self._generate_rdf_sub_section(title, data)

    @override
    def generate_content(self) -> None:
        self._metadata_section()
        for key, val in self.data["item"].GroupedFields():
            self._generate_category_section(key, val)
            break
