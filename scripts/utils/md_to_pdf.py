"""Convert markdown to PDF using fpdf2 with Unicode support."""
from fpdf import FPDF
from fpdf.enums import XPos, YPos
import re

class MarkdownPDF(FPDF):
    def __init__(self):
        super().__init__()
        # Use built-in Helvetica but replace special chars
        self.add_page()
        self.set_auto_page_break(auto=True, margin=15)
        
    def chapter_title(self, title, level=1):
        sizes = {1: 18, 2: 14, 3: 12}
        self.set_font('Helvetica', 'B', sizes.get(level, 12))
        self.set_text_color(44, 62, 80)
        self.cell(0, 10, self.sanitize(title), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        if level == 1:
            self.set_draw_color(52, 152, 219)
            self.line(10, self.get_y(), 200, self.get_y())
        self.ln(3)
        
    def body_text(self, text):
        self.set_font('Helvetica', '', 10)
        self.set_text_color(51, 51, 51)
        self.multi_cell(0, 5, self.sanitize(text))
        self.ln(2)
    
    def code_block(self, code):
        self.set_font('Courier', '', 7)
        self.set_fill_color(44, 62, 80)
        self.set_text_color(236, 240, 241)
        
        for line in code.split('\n'):
            # Sanitize all special characters
            line = self.ascii_box(self.sanitize(line))
            self.cell(0, 4, line[:100], new_x=XPos.LMARGIN, new_y=YPos.NEXT, fill=True)
        self.ln(3)
        self.set_text_color(51, 51, 51)
    
    def ascii_box(self, line):
        """Replace Unicode box-drawing chars with ASCII."""
        replacements = {
            '┌': '+', '┐': '+', '└': '+', '┘': '+',
            '├': '+', '┤': '+', '┬': '+', '┴': '+', '┼': '+',
            '─': '-', '│': '|', '▼': 'v', '◄': '<', '►': '>',
            '→': '->'
        }
        for uni, asc in replacements.items():
            line = line.replace(uni, asc)
        return line
    
    def sanitize(self, text):
        """Replace non-Latin1 characters."""
        result = []
        for c in text:
            if ord(c) < 256:
                result.append(c)
            elif c in '→':
                result.append('->')
            elif c in '≥':
                result.append('>=')
            elif c in '±':
                result.append('+/-')
            else:
                result.append(' ')
        return ''.join(result)
    
    def table_row(self, cells, header=False):
        self.set_font('Helvetica', 'B' if header else '', 9)
        if header:
            self.set_fill_color(52, 152, 219)
            self.set_text_color(255, 255, 255)
        else:
            self.set_fill_color(249, 249, 249)
            self.set_text_color(51, 51, 51)
        
        col_width = 190 / len(cells)
        for cell in cells:
            self.cell(col_width, 7, self.sanitize(str(cell)[:25]), border=1, fill=True)
        self.ln()


def parse_markdown(md_content):
    """Parse markdown and generate PDF."""
    pdf = MarkdownPDF()
    
    lines = md_content.split('\n')
    in_code_block = False
    code_buffer = []
    in_table = False
    
    for line in lines:
        # Code block
        if line.startswith('```'):
            if in_code_block:
                pdf.code_block('\n'.join(code_buffer))
                code_buffer = []
                in_code_block = False
            else:
                in_code_block = True
            continue
            
        if in_code_block:
            code_buffer.append(line)
            continue
        
        # Headers
        if line.startswith('# '):
            pdf.chapter_title(line[2:], 1)
        elif line.startswith('## '):
            pdf.chapter_title(line[3:], 2)
        elif line.startswith('### '):
            pdf.chapter_title(line[4:], 3)
        
        # Table
        elif '|' in line:
            cells = [c.strip() for c in line.split('|')[1:-1]]
            if cells and not all(c.replace('-', '').replace(':', '') == '' for c in cells):
                if not in_table:
                    in_table = True
                    pdf.table_row(cells, header=True)
                else:
                    pdf.table_row(cells, header=False)
        else:
            if in_table:
                in_table = False
                pdf.ln(3)
            
            # Regular text
            if line.strip() and not line.startswith('---'):
                clean = re.sub(r'\*\*(.+?)\*\*', r'\1', line)
                clean = re.sub(r'\*(.+?)\*', r'\1', clean)
                clean = re.sub(r'`(.+?)`', r'\1', clean)
                if clean.strip():
                    pdf.body_text(clean)
    
    return pdf


# Read markdown
with open('docs/pipeline_architecture.md', 'r', encoding='utf-8') as f:
    md_content = f.read()

# Generate PDF
pdf = parse_markdown(md_content)
pdf.output('docs/pipeline_architecture.pdf')
print("PDF generated: docs/pipeline_architecture.pdf")
