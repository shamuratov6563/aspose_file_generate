import re

with open('new_docx2pdf.py', 'r') as f:
    content = f.read()

# 1. Update PDF_DPI
content = re.sub(r'PDF_DPI = 200', 'PDF_DPI = 300', content)

# 2. Update quality and max_width in generate_docs_for_soff
content = re.sub(
    r'(not_pdf_to_images_webp_libreoffice\([^)]+quality=)60(,\s*max_width=)800',
    r'\g<1>85\g<2>1600',
    content
)

content = re.sub(
    r'(pdf_to_images_webp\(\s*temp_path,\s*output_folder,\s*quality=)60(,\s*\))',
    r'\g<1>85, max_width=1600\g<2>',
    content
)

# 3. Update worker count
content = re.sub(
    r'process_doc_poster_generate_queue\(limit=10000, workers=1\)',
    r'workers_env = int(os.environ.get("WORKER_COUNT", "4"))\n    process_doc_poster_generate_queue(limit=10000, workers=workers_env)',
    content
)

# 4. Wrap not_pdf_to_images_webp_libreoffice with try/finally for cleanup
# We find the place after directories are created, insert try:, indent until cleanup, insert finally:

import os
with open('new_docx2pdf.py', 'w') as f:
    f.write(content)

