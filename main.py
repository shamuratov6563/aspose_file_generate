import os
import logging
import aspose.slides as slides


logger = logging.getLogger(__name__)


def pptx_to_pdf(input_path: str, output_dir: str) -> str:
    """
    Convert a PPTX file to PDF.

    Fallback to Aspose.Slides if LibreOffice fails.

    Returns the path to the generated PDF.
    Raises Exception if conversion fails.
    """
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"PPTX file does not exist: {input_path}")

    os.makedirs(output_dir, exist_ok=True)

    filename = os.path.basename(input_path)
    pdf_filename = os.path.splitext(filename)[0] + ".pdf"
    output_path = os.path.join(output_dir, pdf_filename)

    with slides.Presentation(input_path) as pres:
        pres.save(output_path, slides.export.SaveFormat.PDF)
    if os.path.exists(output_path):
        return output_path
    else:
        raise RuntimeError("Aspose conversion did not produce output file.")


pptx_to_pdf("/Users/alishershamuratov/Desktop/ungenerated_docs/documents/2c067618-d9f0-4582-8b34-56dc1d7ace85.pptx", "output_dir")