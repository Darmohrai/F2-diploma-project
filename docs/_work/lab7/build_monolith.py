"""Builds the monolith of the explanatory note, chapters 1 and 2, as one Word document.

    python docs/_work/lab7/build_monolith.py [--puppeteer pp.json]

Input:  docs/1_analysis.md, docs/2_design.md, docs/uml/kurin-use-cases.png (if exported from StarUML).
Output: docs/F2_Diploma_Chapters1-2_Дармограй.docx

Needs pandoc 3, python-docx and mermaid-cli (mmdc) with a headless Chrome. Mermaid blocks become PNG figures
numbered "Рисунок 2.N"; links between Markdown files become plain text, because they do not resolve inside Word.
Formatting follows chapter 1 (docs/_work/lab4/build_chapter.py): A4, margins 2.5 / 1.5 / 2 / 2 cm,
Times New Roman 14 pt, line spacing 1.5, first-line indent 1.25 cm.
"""
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from PIL import Image
from docx import Document
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_LINE_SPACING
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Cm, Pt, RGBColor

ROOT = Path(__file__).resolve().parents[3]
DOCS = ROOT / "docs"
OUT = DOCS / "F2_Diploma_Chapters1-2_Дармограй.docx"

MERMAID = re.compile(r"```mermaid\n(.*?)```", re.S)
ANCHOR = re.compile(r'<a id="[^"]*"></a>\n?')
REL_LINK = re.compile(r"\[([^\]]+)\]\((?!https?://)[^)]*\)")


HEADING = re.compile(r"^#{2,4} (?:[\d.]+\s)?(.+)$", re.M)


def render_figures(md: str, chapter: int, tmp: Path, puppeteer: str | None) -> str:
    """Mermaid blocks and the StarUML picture, numbered in order; the caption is the nearest heading above."""
    uml = DOCS / "uml" / "kurin-use-cases.png"
    counter = {"n": 0}
    used: dict[str, int] = {}

    def repl(m):
        code = m.group(1)
        before = md[:m.start()]
        heads = HEADING.findall(before)
        title = heads[-1].strip().strip("`") if heads else "Діаграма"
        used[title] = used.get(title, 0) + 1
        if used[title] > 1:
            title += f" (частина {used[title]})"
        if code.strip() == "%%UML%%":
            if not uml.exists():
                counter["n"] += 1
                return ("\n**[Вставити тут PNG з StarUML: docs/uml/kurin-use-cases.png]**\n\n"
                        f"::: {{custom-style=\"Caption\"}}\nРисунок {chapter}.{counter['n']} – "
                        "Діаграма прецедентів Kurin (StarUML)\n:::\n")
            png = uml
            title = "Діаграма прецедентів Kurin (StarUML)"
        else:
            src = tmp / f"ch{chapter}_{counter['n'] + 1}.mmd"
            png = tmp / f"ch{chapter}_{counter['n'] + 1}.png"
            src.write_text(code, encoding="utf-8")
            cmd = ["mmdc", "-i", str(src), "-o", str(png), "-s", "2", "-b", "white", "-w", "1600"]
            if puppeteer:
                cmd += ["-p", puppeteer]
            subprocess.run(cmd, check=True, capture_output=True)
        counter["n"] += 1
        cap = f"Рисунок {chapter}.{counter['n']} – {title}"
        with Image.open(png) as im:
            w, h = im.size
        size = "width=16.5cm" if h / w <= 22.0 / 16.5 else "height=22cm"
        return f"\n![]({png.as_posix()}){{{size}}}\n\n::: {{custom-style=\"Caption\"}}\n{cap}\n:::\n"

    return MERMAID.sub(repl, md)


def prepare(md: str, chapter: int, tmp: Path, puppeteer: str | None) -> str:
    md = ANCHOR.sub("", md)
    md = re.sub(r"!\[[^\]]*\]\(\./uml/kurin-use-cases\.png\)", "```mermaid\n%%UML%%\n```", md)
    md = render_figures(md, chapter, tmp, puppeteer)
    md = REL_LINK.sub(lambda m: m.group(1), md)
    md = md.replace("<br/>", " ").replace("<br>", " ")
    # pandoc keeps the backslash of \| inside a code span of a table cell: such spans become plain text
    md = re.sub(r"(?m)^\|.*$", lambda row: re.sub(r"`([^`]*\\\|[^`]*)`", lambda c: c.group(1), row.group(0)), md)
    return md


def set_cell_borders(table):
    tbl = table._tbl
    pr = tbl.tblPr
    borders = OxmlElement("w:tblBorders")
    for edge in ("top", "left", "bottom", "right", "insideH", "insideV"):
        el = OxmlElement(f"w:{edge}")
        el.set(qn("w:val"), "single")
        el.set(qn("w:sz"), "4")
        el.set(qn("w:color"), "000000")
        borders.append(el)
    pr.append(borders)


def set_widths(table, total_cm):
    """Column widths in proportion to the square root of the longest text of each column."""
    cols = len(table.columns)
    weight = [1.0] * cols
    for row in table.rows:
        for i, cell in enumerate(row.cells[:cols]):
            weight[i] = max(weight[i], len(cell.text) ** 0.5)
    weight = [max(w, 2.2) for w in weight]
    factor = total_cm / sum(weight)
    tbl_pr = table._tbl.tblPr
    layout = OxmlElement("w:tblLayout")
    layout.set(qn("w:type"), "fixed")
    tbl_pr.append(layout)
    grid = table._tbl.tblGrid
    for i, gc in enumerate(grid.findall(qn("w:gridCol"))):
        gc.set(qn("w:w"), str(int(Cm(weight[i] * factor).twips)))
    for row in table.rows:
        for i, cell in enumerate(row.cells[:cols]):
            cell.width = Cm(weight[i] * factor)


def style_document(path: Path):
    doc = Document(str(path))
    for section in doc.sections:
        section.page_height, section.page_width = Cm(29.7), Cm(21.0)
        section.left_margin, section.right_margin = Cm(2.5), Cm(1.5)
        section.top_margin = section.bottom_margin = Cm(2.0)

    for st in doc.styles:
        if st.type != 1:  # paragraph and character styles only
            continue
        try:
            st.font.name = "Times New Roman"
            st.font.color.rgb = RGBColor(0, 0, 0)
            rpr = st.element.get_or_add_rPr()
            fonts = rpr.find(qn("w:rFonts"))
            if fonts is None:
                fonts = OxmlElement("w:rFonts")
                rpr.append(fonts)
            for attr in ("w:ascii", "w:hAnsi", "w:cs", "w:eastAsia"):
                fonts.set(qn(attr), "Times New Roman")
            for attr in ("w:asciiTheme", "w:hAnsiTheme", "w:cstheme", "w:eastAsiaTheme"):
                if fonts.get(qn(attr)) is not None:
                    del fonts.attrib[qn(attr)]
        except (AttributeError, ValueError):
            pass

    sizes = {"Heading 1": 16, "Heading 2": 14, "Heading 3": 14, "Heading 4": 14}
    for name, size in sizes.items():
        if name in doc.styles:
            st = doc.styles[name]
            st.font.size = Pt(size)
            st.font.bold = True
            st.font.italic = False
            st.paragraph_format.space_before = Pt(12)
            st.paragraph_format.space_after = Pt(6)
            st.paragraph_format.keep_with_next = True

    for par in doc.paragraphs:
        name = par.style.name
        pf = par.paragraph_format
        if name.startswith("Heading"):
            if name == "Heading 1":
                par.alignment = WD_ALIGN_PARAGRAPH.CENTER
            continue
        if name == "Caption":
            par.alignment = WD_ALIGN_PARAGRAPH.CENTER
            pf.space_after = Pt(12)
            for run in par.runs:
                run.font.size = Pt(14)
                run.font.italic = False
            continue
        if any(r._element.find(qn("w:drawing")) is not None for r in par.runs):
            par.alignment = WD_ALIGN_PARAGRAPH.CENTER
            pf.first_line_indent = Cm(0)
            continue
        for run in par.runs:
            if run.font.name in ("Consolas", "Courier New") or (run.style is not None and "Verbatim" in run.style.name):
                run.font.size = Pt(12)
            else:
                run.font.size = Pt(14)
        pf.line_spacing_rule = WD_LINE_SPACING.ONE_POINT_FIVE
        pf.space_after = Pt(0)
        if name in ("Body Text", "First Paragraph", "Normal"):
            pf.first_line_indent = Cm(1.25)
            par.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY

    for table in doc.tables:
        table.alignment = WD_TABLE_ALIGNMENT.CENTER
        set_cell_borders(table)
        set_widths(table, 17.0)
        for i, row in enumerate(table.rows):
            for cell in row.cells:
                for par in cell.paragraphs:
                    par.paragraph_format.first_line_indent = Cm(0)
                    par.paragraph_format.line_spacing = 1.0
                    for run in par.runs:
                        mono = run.style is not None and "Verbatim" in run.style.name
                        run.font.size = Pt(10 if mono else 12)
                        if i == 0:
                            run.font.bold = True
    doc.save(str(path))


def main():
    puppeteer = None
    if "--puppeteer" in sys.argv:
        puppeteer = sys.argv[sys.argv.index("--puppeteer") + 1]
    if shutil.which("pandoc") is None or shutil.which("mmdc") is None:
        sys.exit("pandoc and mmdc (mermaid-cli) are required")
    with tempfile.TemporaryDirectory() as t:
        tmp = Path(t)
        ch1 = prepare((DOCS / "1_analysis.md").read_text(encoding="utf-8"), 1, tmp, puppeteer)
        ch2 = prepare((DOCS / "2_design.md").read_text(encoding="utf-8"), 2, tmp, puppeteer)
        combined = tmp / "monolith.md"
        combined.write_text(ch1.rstrip() + "\n\n```{=openxml}\n<w:p><w:r><w:br w:type=\"page\"/></w:r></w:p>\n```\n\n"
                            + ch2, encoding="utf-8")
        subprocess.run(["pandoc", str(combined), "-f", "markdown+pipe_tables+fenced_divs+raw_attribute-tex_math_dollars",
                        "-t", "docx", "-o", str(OUT), "--resource-path", str(tmp)], check=True)
    style_document(OUT)
    print(OUT)


if __name__ == "__main__":
    main()
