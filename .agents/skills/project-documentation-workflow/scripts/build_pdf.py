"""
Project Documentation Workflow - ReportLab PDF Compiler
========================================================
Compiles all 31 documentation content files from documentation/content/
into a publication-grade, professional PDF document.
Features:
- Custom Running Headers & "Page X of Y" Footers (NumberedCanvas)
- Professional Cover Page & Document Control styling
- Styled Markdown tables, code blocks, callouts, and typography
- Clean page breaks between major sections
"""

import glob
import os
import re
import sys
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    PageBreak,
    KeepTogether,
    Preformatted
)
from reportlab.pdfgen import canvas

# ------------------------------------------------------------------------------
# Custom Canvas for Page Numbering ("Page X of Y") & Running Headers
# ------------------------------------------------------------------------------
class NumberedCanvas(canvas.Canvas):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._saved_page_states = []

    def showPage(self):
        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        num_pages = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)
            self.draw_page_decorations(num_pages)
            super().showPage()
        super().save()

    def draw_page_decorations(self, page_count):
        if self._pageNumber == 1:
            # Suppress header and footer on cover page
            return

        self.saveState()
        self.setFont("Helvetica", 8)
        self.setFillColor(colors.HexColor("#64748B"))

        # Running Header
        self.drawString(54, 11 * inch - 36, "Real-Time Tech Job Market Intelligence Platform — Engineering Specification")
        self.setStrokeColor(colors.HexColor("#CBD5E1"))
        self.setLineWidth(0.5)
        self.line(54, 11 * inch - 42, 8.5 * inch - 54, 11 * inch - 42)

        # Running Footer
        page_str = f"Page {self._pageNumber} of {page_count}"
        self.drawRightString(8.5 * inch - 54, 36, page_str)
        self.drawString(54, 36, "CONFIDENTIAL & PROPRIETARY — BIG DATA ENGINEERING TEAM")
        self.line(54, 46, 8.5 * inch - 54, 46)

        self.restoreState()


# ------------------------------------------------------------------------------
# Markdown to ReportLab Flowable Parser
# ------------------------------------------------------------------------------
def sanitize_text(text: str) -> str:
    """Escapes XML entities while preserving ReportLab tags."""
    text = text.replace("&", "&amp;")
    text = text.replace("<", "&lt;").replace(">", "&gt;")
    # Restore basic formatting
    text = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', text)
    text = re.sub(r'\*(.*?)\*', r'<i>\1</i>', text)
    text = re.sub(r'`(.*?)`', r'<font face="Courier" color="#1E3A8A"><b>\1</b></font>', text)
    return text

def parse_markdown_to_flowables(md_content: str, styles: dict, is_cover: bool = False) -> list:
    flowables = []
    lines = md_content.split('\n')
    in_code_block = False
    code_lines = []
    in_table = False
    table_lines = []

    for line in lines:
        # Fenced Code Block
        if line.strip().startswith('```'):
            if in_code_block:
                in_code_block = False
                code_text = "\n".join(code_lines)
                flowables.append(Preformatted(code_text, styles['CodeBlock']))
                flowables.append(Spacer(1, 6))
                code_lines = []
            else:
                in_code_block = True
            continue

        if in_code_block:
            code_lines.append(line)
            continue

        # Markdown Table Detection
        if '|' in line and not line.strip().startswith('>'):
            in_table = True
            table_lines.append(line)
            continue
        elif in_table:
            # End of table block
            in_table = False
            t_flowable = parse_table(table_lines, styles)
            if t_flowable:
                flowables.append(t_flowable)
                flowables.append(Spacer(1, 8))
            table_lines = []

        line_str = line.strip()
        if not line_str:
            continue

        # Headings
        if line_str.startswith('# '):
            h_text = sanitize_text(line_str[2:].strip())
            flowables.append(Spacer(1, 10))
            flowables.append(Paragraph(h_text, styles['DocH1']))
            flowables.append(Spacer(1, 6))
        elif line_str.startswith('## '):
            h_text = sanitize_text(line_str[3:].strip())
            flowables.append(Spacer(1, 8))
            flowables.append(Paragraph(h_text, styles['DocH2']))
            flowables.append(Spacer(1, 4))
        elif line_str.startswith('### '):
            h_text = sanitize_text(line_str[4:].strip())
            flowables.append(Spacer(1, 6))
            flowables.append(Paragraph(h_text, styles['DocH3']))
            flowables.append(Spacer(1, 3))
        # Blockquote / Callout
        elif line_str.startswith('>'):
            callout_text = sanitize_text(line_str.lstrip('> ').strip())
            t = Table([[Paragraph(f"<b>NOTE:</b> {callout_text}", styles['Callout'])]], colWidths=[7.0 * inch])
            t.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#EFF6FF')),
                ('LEFTPADDING', (0, 0), (-1, -1), 12),
                ('RIGHTPADDING', (0, 0), (-1, -1), 12),
                ('TOPPADDING', (0, 0), (-1, -1), 6),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('LINELEFT', (0, 0), (0, 0), 3, colors.HexColor('#2563EB')),
                ('BOX', (0, 0), (-1, -1), 0.5, colors.HexColor('#DBEAFE')),
            ]))
            flowables.append(t)
            flowables.append(Spacer(1, 6))
        # Bullet List
        elif line_str.startswith(('-', '*')):
            bullet_text = sanitize_text(line_str.lstrip('-* ').strip())
            flowables.append(Paragraph(f"• &nbsp; {bullet_text}", styles['DocBullet']))
        # Regular Paragraph
        else:
            p_text = sanitize_text(line_str)
            flowables.append(Paragraph(p_text, styles['DocBody']))
            flowables.append(Spacer(1, 4))

    # Catch trailing table
    if in_table and table_lines:
        t_flowable = parse_table(table_lines, styles)
        if t_flowable:
            flowables.append(t_flowable)
            flowables.append(Spacer(1, 8))

    return flowables


def parse_table(table_lines: list, styles: dict) -> Table:
    """Parses markdown table lines into a ReportLab Table."""
    raw_data = []
    for line in table_lines:
        if re.match(r'^\s*\|?\s*[-:\s|]+\s*\|?\s*$', line):
            # Skip separator line
            continue
        cells = [c.strip() for c in line.split('|')]
        if cells and cells[0] == '':
            cells = cells[1:]
        if cells and cells[-1] == '':
            cells = cells[:-1]
        if cells:
            raw_data.append(cells)

    if not raw_data:
        return None

    # Normalize row lengths
    max_cols = max(len(row) for row in raw_data)
    table_data = []
    for r_idx, row in enumerate(raw_data):
        while len(row) < max_cols:
            row.append("")
        cell_paragraphs = []
        for cell in row:
            clean_c = sanitize_text(cell)
            if r_idx == 0:
                p = Paragraph(f"<b>{clean_c}</b>", styles['TableHeader'])
            else:
                p = Paragraph(clean_c, styles['TableCell'])
            cell_paragraphs.append(p)
        table_data.append(cell_paragraphs)

    col_width = (7.0 * inch) / max(max_cols, 1)
    col_widths = [col_width] * max_cols

    t = Table(table_data, colWidths=col_widths, repeatRows=1)
    t.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1E3A8A')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('TOPPADDING', (0, 0), (-1, -1), 4),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ('LEFTPADDING', (0, 0), (-1, -1), 6),
        ('RIGHTPADDING', (0, 0), (-1, -1), 6),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#F8FAFC')]),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#CBD5E1')),
    ]))
    return t


# ------------------------------------------------------------------------------
# PDF Builder Orchestrator
# ------------------------------------------------------------------------------
def build_pdf_document(root_dir: str = ".") -> str:
    root_dir = os.path.abspath(root_dir)
    content_dir = os.path.join(root_dir, "documentation", "content")
    out_dir = os.path.join(root_dir, "documentation", "generated")
    os.makedirs(out_dir, exist_ok=True)
    pdf_path = os.path.join(out_dir, "Project_Documentation.pdf")

    # Define Typography & Styles
    base_styles = getSampleStyleSheet()
    styles = {
        'DocH1': ParagraphStyle(
            'DocH1',
            parent=base_styles['Heading1'],
            fontName='Helvetica-Bold',
            fontSize=16,
            leading=20,
            textColor=colors.HexColor('#1E3A8A'),
            keepWithNext=True
        ),
        'DocH2': ParagraphStyle(
            'DocH2',
            parent=base_styles['Heading2'],
            fontName='Helvetica-Bold',
            fontSize=12,
            leading=16,
            textColor=colors.HexColor('#2563EB'),
            keepWithNext=True
        ),
        'DocH3': ParagraphStyle(
            'DocH3',
            parent=base_styles['Heading3'],
            fontName='Helvetica-Bold',
            fontSize=10,
            leading=13,
            textColor=colors.HexColor('#1E293B'),
            keepWithNext=True
        ),
        'DocBody': ParagraphStyle(
            'DocBody',
            parent=base_styles['BodyText'],
            fontName='Helvetica',
            fontSize=8.5,
            leading=12,
            textColor=colors.HexColor('#334155')
        ),
        'DocBullet': ParagraphStyle(
            'DocBullet',
            parent=base_styles['BodyText'],
            fontName='Helvetica',
            fontSize=8.5,
            leading=12,
            leftIndent=12,
            textColor=colors.HexColor('#334155')
        ),
        'TableHeader': ParagraphStyle(
            'TableHeader',
            fontName='Helvetica-Bold',
            fontSize=8,
            leading=10,
            textColor=colors.white
        ),
        'TableCell': ParagraphStyle(
            'TableCell',
            fontName='Helvetica',
            fontSize=7.5,
            leading=9.5,
            textColor=colors.HexColor('#1E293B')
        ),
        'Callout': ParagraphStyle(
            'Callout',
            fontName='Helvetica',
            fontSize=8,
            leading=11,
            textColor=colors.HexColor('#1E3A8A')
        ),
        'CodeBlock': ParagraphStyle(
            'CodeBlock',
            fontName='Courier',
            fontSize=7,
            leading=9,
            textColor=colors.HexColor('#0F172A')
        )
    }

    doc = SimpleDocTemplate(
        pdf_path,
        pagesize=letter,
        leftMargin=54,
        rightMargin=54,
        topMargin=54,
        bottomMargin=54
    )

    story = []

    # Read all 31 files in order
    md_files = sorted(glob.glob(os.path.join(content_dir, "*.md")))
    if not md_files:
        from generate_doc import generate_all_sections
        generate_all_sections(root_dir)
        md_files = sorted(glob.glob(os.path.join(content_dir, "*.md")))

    print(f"[INFO] Compiling {len(md_files)} documentation sections into PDF...")

    for idx, fpath in enumerate(md_files):
        with open(fpath, 'r', encoding='utf-8') as fp:
            content = fp.read()

        is_cover = (idx == 0)
        flowables = parse_markdown_to_flowables(content, styles, is_cover=is_cover)
        story.extend(flowables)

        # Insert PageBreak after sections (except on the last section)
        if idx < len(md_files) - 1:
            story.append(PageBreak())

    # Build PDF with NumberedCanvas
    doc.build(story, canvasmaker=NumberedCanvas)
    print(f"[SUCCESS] Master Documentation PDF generated at:\n  --> {pdf_path}")
    return pdf_path

if __name__ == "__main__":
    build_pdf_document()
