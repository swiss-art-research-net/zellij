import io
import os
from abc import ABC, abstractmethod
from datetime import date
from urllib.parse import urlparse

from fpdf import FPDF, Align
from fpdf.enums import TextEmphasis, WrapMode
from fpdf.outline import TableOfContents

BASE_URL = os.getenv("BASE_URL", "http://localhost:5000")
FONT = "titillium-web"


class CustomPDF(FPDF):
    def __init__(
        self,
        name: str,
        institution: str | None = None,
        version: str | None = None,
    ):
        super().__init__(
            orientation="landscape",
            unit="mm",
            format="a4",
            font_cache_dir="DEPRECATED",
        )
        self.name = name
        self.institution = institution
        self.version = version

    def header(self):
        if self.page_no() == 1:
            return  # Skip header on the first page

        self.set_font(FONT, size=12)

        if self.institution:
            header_text = (
                f"Semantic Documentation for {self.name} curated by {self.institution}"
            )
        else:
            header_text = f"Semantic Documentation for {self.name}"

        self.cell(
            text=header_text,
            align=Align.C,
            center=True,
        )
        self.ln(10)

    def footer(self):
        if self.page_no() == 1:
            return  # Skip footer on the first page

        self.set_y(-12)
        self.set_font(FONT, size=12)
        if self.version:
            self.cell(
                text=f"Version Number: {self.version}",
                align=Align.C,
                center=True,
            )
        self.ln(5)
        self.cell(
            text=f"Generation Date: {date.today().strftime('%d/%m/%Y')} |",
            align=Align.R,
            w=100,
        )
        self.cell(
            text="Generated Via: Zellij Semantic Pattern Documentation Tool",
            link=BASE_URL,
            align=Align.L,
            w=100,
        )


class PDFExporter(ABC):
    def __init__(self, id: str, model_id: str | None = None):
        self.id = id
        self.model_id = model_id
        self.data = {}
        self.font = FONT
        self._inserted_toc = False

    @abstractmethod
    def get_file_name(self) -> str: ...

    @abstractmethod
    def load_data(self) -> dict: ...

    @abstractmethod
    def generate_content(self) -> None: ...

    def reset_font(self) -> None:
        self.pdf.set_font(self.font, size=12)

    def h1(self, text: str) -> None:
        self.pdf.set_font(self.font, style="B", size=32)
        self.pdf.multi_cell(w=0, text=text + "\n", center=True, align=Align.C)
        self.reset_font()

    def div(
        self,
        text: str,
        align: Align = Align.L,
        size: int = 12,
        decoration: TextEmphasis = TextEmphasis.NONE,
    ) -> None:
        self.pdf.set_font(self.font, size=size, style=decoration)
        self.pdf.multi_cell(w=0, text=text + "\n", center=align == Align.C, align=align)
        self.reset_font()

    def table(
        self,
        rows: tuple,
        has_header=True,
        sizing: tuple[int, ...] | None = None,
        font_size: int = 8,
    ) -> None:
        self.pdf.set_font(self.font, size=font_size)
        with self.pdf.table(
            first_row_as_headings=has_header,
            repeat_headings=False,
            wrapmode=WrapMode.CHAR,
            col_widths=sizing,
        ) as table:
            header = rows[0] if has_header else []
            for data_row in rows:
                row = table.row()
                for idx, cell in enumerate(data_row):
                    if not isinstance(cell, int) and len(cell) == 0:
                        content = "N/A"
                    elif isinstance(cell, list):
                        content = ", ".join(cell)
                    else:
                        content = str(cell)

                    if urlparse(content).scheme in ("http", "https") and (
                        (len(header) > 0 and header[idx] != "URI")
                        and data_row[0] != "URI"
                        and data_row[0] != "Namespace"
                    ):
                        self.pdf.set_text_color(0, 0, 238)
                        row.cell(
                            "Link",
                            link=content,
                            align=Align.L,
                        )
                        self.pdf.set_text_color(0, 0, 0)
                    else:
                        row.cell(content)

        self.reset_font()

    def section(self, title: str) -> None:
        self.pdf.add_page()
        if not self._inserted_toc:
            self.pdf.insert_toc_placeholder(
                TableOfContents().render_toc, allow_extra_pages=True
            )
            self._inserted_toc = True
        self.pdf.start_section(title)

    def sub_section(self, title: str, level=1) -> None:
        self.pdf.start_section(title, level=level)

    def export(self) -> io.BytesIO:
        try:
            metadata = self.load_data()
        except Exception as e:
            raise RuntimeError(f"Failed to load data: {e}")

        self.pdf = CustomPDF(
            name=metadata.get("name", ""),
            institution=metadata.get("inistitution", ""),
            version=metadata.get("version"),
        )
        self.pdf.set_auto_page_break(True, 15)
        self.pdf.add_font(
            "titillium-web", fname="./website/static/TitilliumWeb-Regular.ttf"
        )
        self.pdf.add_font(
            "titillium-web", style="B", fname="./website/static/TitilliumWeb-Bold.ttf"
        )
        self.reset_font()
        self.pdf.add_page()

        self.generate_content()

        pdf_bytes = self.pdf.output()

        file = io.BytesIO()
        file.write(pdf_bytes)
        file.seek(0)

        return file
