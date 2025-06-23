import io
from abc import ABC, abstractmethod

from fpdf import FPDF, Align
from fpdf.enums import TextEmphasis, WrapMode
from fpdf.outline import TableOfContents


class PDFExporter(ABC):
    def __init__(self, id: str):
        self.id = id
        self.data = {}
        self.font = "helvetica"
        self._inserted_toc = False

    @abstractmethod
    def load_data(self) -> None: ...

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
            self.load_data()
        except Exception as e:
            raise RuntimeError(f"Failed to load data: {e}")

        self.pdf = FPDF(format="A4", orientation="landscape")
        self.pdf.set_auto_page_break(True)
        self.reset_font()
        self.pdf.add_page()

        self.generate_content()

        pdf_bytes = self.pdf.output()

        file = io.BytesIO()
        file.write(pdf_bytes)
        file.seek(0)

        return file
