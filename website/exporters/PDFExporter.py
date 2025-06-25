import io
from abc import ABC, abstractmethod
from datetime import date

from fpdf import FPDF, Align
from fpdf.enums import TextEmphasis, WrapMode
from fpdf.outline import TableOfContents


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

        self.set_font("helvetica", size=12)

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
        self.set_font("helvetica", size=12)
        if self.version:
            self.cell(
                text=f"Version Number: {self.version}",
                align=Align.C,
                center=True,
            )
        self.ln(5)
        self.cell(
            text=f"Generation Date: {date.today().strftime('%d/%m/%Y')} | Generated Via: Zellij Semantic Pattern Documentation Tool",
            align=Align.C,
            center=True,
        )


class PDFExporter(ABC):
    def __init__(self, id: str):
        self.id = id
        self.data = {}
        self.font = "helvetica"
        self._inserted_toc = False

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

    def table(self, rows: tuple, has_header=True) -> None:
        self.pdf.set_font(self.font, size=8)
        with self.pdf.table(
            first_row_as_headings=has_header,
            repeat_headings=False,
            wrapmode=WrapMode.CHAR,
        ) as table:
            for data_row in rows:
                row = table.row()
                for cell in data_row:
                    if not isinstance(cell, int) and len(cell) == 0:
                        content = "N/A"
                    elif isinstance(cell, list):
                        content = ", ".join(cell)
                    else:
                        content = str(cell)

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
        self.pdf.set_auto_page_break(True)
        self.reset_font()
        self.pdf.add_page()

        self.generate_content()

        pdf_bytes = self.pdf.output()

        file = io.BytesIO()
        file.write(pdf_bytes)
        file.seek(0)

        return file
