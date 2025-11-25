import os
import shutil
import subprocess
import tempfile
import zipfile
import glob
import requests
from PIL import Image
from pdf2image import convert_from_path
from multiprocessing import Process, Queue, cpu_count
import time
import pathlib
import subprocess
from dotenv import load_dotenv
import os

load_dotenv()  # This loads variables from a .env file in the current directory

# Access environment variables
TOKEN = os.getenv("TOKEN")
BASE_URL = os.getenv("BASE_URL")

headers = {'Authorization': f"Bearer {TOKEN}"}


def not_pdf_to_images_webp_libreoffice(ppt_path, output_folder, quality=15, max_width=800, max_slides=4):
    abs_ppt = os.path.abspath(ppt_path)
    abs_output = tempfile.mkdtemp(prefix="libreoffice_out_")
    os.makedirs(output_folder, exist_ok=True)

    profile_dir = tempfile.mkdtemp(prefix="libreoffice_profile_")
    result = subprocess.run([
        "soffice",
        "--headless",
        "--norestore",
        "--nolockcheck",
        "--nodefault",
        f"-env:UserInstallation=file://{profile_dir}",
        "--convert-to", "pdf",
        "--outdir", abs_output,
        abs_ppt
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=600)

    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"LibreOffice failed: {result.stderr}\n{result.stdout}")

    # Find any PDF in the output folder
    pdf_candidates = glob.glob(os.path.join(abs_output, "*.pdf"))
    if not pdf_candidates:
        raise RuntimeError(f"No PDF generated in {abs_output}. LibreOffice stdout: {result.stdout} stderr: {result.stderr}")

    pdf_path = pdf_candidates[0]  # pick first PDF
    print(f"📄 Using generated PDF: {pdf_path}")

    pages = convert_from_path(pdf_path, dpi=200)
    saved_paths = []

    for i, pil_img in enumerate(pages[:max_slides], start=1):
        if pil_img.width > max_width:
            ratio = max_width / pil_img.width
            new_height = int(pil_img.height * ratio)
            pil_img = pil_img.resize((max_width, new_height), Image.LANCZOS)

        webp_path = os.path.join(output_folder, f"slide_{i}.webp")
        pil_img.convert("RGB").save(webp_path, "webp", quality=quality if i == 1 else 5, method=6)
        saved_paths.append(webp_path)

    # Cleanup
    shutil.rmtree(abs_output, ignore_errors=True)
    shutil.rmtree(profile_dir, ignore_errors=True)

    return saved_paths, len(pages)


def pdf_to_images_webp(pdf_path, output_folder, quality=60, max_width=None):
    os.makedirs(output_folder, exist_ok=True)
    images = convert_from_path(pdf_path)

    for i, img in enumerate(images):
        if max_width and img.width > max_width:
            ratio = max_width / float(img.width)
            new_height = int(float(img.height) * ratio)
            img = img.resize((max_width, new_height))

        img_path = os.path.join(output_folder, f"page_{i+1}.webp")
        img.save(img_path, "WEBP", quality=quality)

    return [os.path.join(output_folder, f"page_{i+1}.webp") for i in range(len(images))], len(images)


def download_file(file_url, save_path):
    response = requests.get(file_url)
    response.raise_for_status()
    with open(save_path, 'wb') as f:
        f.write(response.content)


from lxml import etree as ET


def create_placeholder(path: str, size=(100, 100)):
    """Create a white placeholder image."""
    img = Image.new("RGB", size, color=(255, 255, 255))
    os.makedirs(os.path.dirname(path), exist_ok=True)
    img.save(path, "PNG")


def clean_xml_references(temp_dir: str, missing_files: list[str]):
    """Remove broken image references in XML files."""
    for root_dir, _, files in os.walk(temp_dir):
        for file in files:
            if file.endswith(".xml") or file.endswith(".rels"):
                xml_path = os.path.join(root_dir, file)
                try:
                    tree = ET.parse(xml_path)
                    root = tree.getroot()

                    # Remove <a:blip> with missing r:embed
                    for blip in root.findall(".//{*}blip"):
                        rid = blip.attrib.get(
                            "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}embed"
                        )
                        if rid and any(m in rid for m in missing_files):
                            parent = blip.getparent() if hasattr(blip, "getparent") else None
                            if parent is not None:
                                parent.remove(blip)

                    # Remove <Relationship> entries pointing to missing files
                    for rel in root.findall(".//{*}Relationship"):
                        target = rel.attrib.get("Target")
                        if target and any(m in target for m in missing_files):
                            parent = rel.getparent() if hasattr(rel, "getparent") else None
                            if parent is not None:
                                parent.remove(rel)

                    tree.write(xml_path)
                except Exception as e:
                    print(f"⚠️ Failed to clean {xml_path}: {e}")


def try_repair_office_file(path: str) -> str | None:
    """
    Repair corrupted Office file (pptx/docx).
    - For pptx/docx: replace broken images with placeholders and clean XML references.
    - For ppt: try auto-convert to pptx via LibreOffice, then repair.
    Returns repaired file path or None if repair failed.
    """
    # Handle old .ppt files by converting to .pptx first
    if path.lower().endswith(".ppt") and not path.lower().endswith(".pptx"):
        pptx_path = path.replace(".ppt", ".pptx")
        try:
            subprocess.run(
                ["soffice", "--headless", "--convert-to", "pptx", path],
                check=True
            )
            if os.path.exists(pptx_path):
                print(f"🌀 Converted old PPT → PPTX: {pptx_path}")
                path = pptx_path
            else:
                print(f"❌ Failed to convert {path} to PPTX")
                return None
        except Exception as e:
            print(f"❌ LibreOffice conversion failed for {path}: {e}")
            return None

    if not zipfile.is_zipfile(path):
        print(f"⚠️ {path} is not a valid zip-based Office file, cannot repair.")
        return None

    missing_files = []
    try:
        temp_dir = tempfile.mkdtemp(prefix="repair_office_")

        with zipfile.ZipFile(path, 'r') as zip_ref:
            for file in zip_ref.namelist():
                try:
                    zip_ref.extract(file, temp_dir)
                except Exception as e:
                    if file.lower().endswith((".png", ".jpg", ".jpeg")):
                        print(f"⚠️ Replacing corrupted image with placeholder: {file}")
                        out_path = os.path.join(temp_dir, file)
                        create_placeholder(out_path)
                        missing_files.append(file)
                    else:
                        print(f"⚠️ Skipping corrupted non-image file: {file} ({e})")
                        missing_files.append(file)

        # Clean XML references to missing files
        if missing_files:
            clean_xml_references(temp_dir, missing_files)

        # Build repaired path
        repaired_path = (
            path.replace(".pptx", "_repaired.pptx")
                .replace(".docx", "_repaired.docx")
        )

        base = repaired_path.replace(".pptx", "").replace(".docx", "")
        shutil.make_archive(base, "zip", temp_dir)

        # Rename back to docx/pptx
        zip_file = base + ".zip"
        if os.path.exists(zip_file):
            shutil.move(zip_file, repaired_path)

        print(f"🛠️ Repaired file saved: {repaired_path}")
        return repaired_path

    except Exception as e:
        print(f"❌ Repair attempt failed for {path}: {e}")
        return None
    

def generate_docs_for_soff(doc_id):
    temp_path = None
    output_folder = None
    try:
        response = requests.get(f'{BASE_URL}/api/v1/seller/admin/product-list/{doc_id}/', headers=headers, stream=True)
        data = response.json()
        file_type = data['document']['file_type'].lower()
        file_url = data['document']['file_url']
        if not file_url:
            return True
        temp_path = f"temp_copy_{doc_id}{file_type}"
        output_folder = f"images_slide_copy_{doc_id}"

        image_path = []

        download_file(file_url, temp_path)
        repaired = None
        if file_type in ['.pptx', '.ppt', '.doc', '.docx'] and os.path.exists(temp_path):
            try:
                image_paths, pages_count = not_pdf_to_images_webp_libreoffice(temp_path, output_folder, quality=60, max_width=800)

            except Exception as e:
                print(f"⚠️ LibreOffice failed on {temp_path}: {e}")
                repaired = try_repair_office_file(temp_path)
                if repaired:
                    image_paths, pages_count = not_pdf_to_images_webp_libreoffice(repaired, output_folder, quality=60, max_width=800)
                else:
                    print(f"❌ Skipping doc_id={doc_id}, corrupted file.")
                    return False
            finally:
                if repaired and os.path.exists(repaired):
                    os.remove(repaired)
                if os.path.exists(temp_path):
                    os.remove(temp_path)

        elif file_type == '.pdf':
            image_paths, pages_count = pdf_to_images_webp(temp_path, output_folder, quality=60)
            image_paths = image_paths[:3]
        else:
            return True

        # Upload images back
        print(len(image_path), 'len')
        files = []
        for path in image_paths:
            files.append(("images", (os.path.basename(path), open(path, "rb"), "image/webp")))

        data = {'page_count': pages_count}

        requests.patch(f"{BASE_URL}/api/v1/seller/admin/product-list/{doc_id}/", files=files, headers=headers, data=data)
        print(f"✅ Finished doc_id={doc_id}")

    except Exception as e:
        print(f"❌ Error doc_id={doc_id}: {e}")

    finally:
        # Cleanup temp files
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)
        if output_folder and os.path.exists(output_folder):
            shutil.rmtree(output_folder)

    return True
# ========= Worker & Queue System =========


def worker(queue: Queue):
    while True:
        doc_id = queue.get()
        if doc_id is None:  # Poison pill -> stop worker
            break
        generate_docs_for_soff(doc_id)


def process_doc_poster_generate_queue(limit=100, workers=None):
    workers = workers or max(2, cpu_count() // 2)
    queue = Queue(maxsize=workers * 2)

    # Start workers
    processes = [Process(target=worker, args=(queue,)) for _ in range(workers)]
    for p in processes:
        p.start()

    start = 660546

    for _ in range(limit):
        endpoint = f"{BASE_URL}/api/v1/seller/moderation-change/?type=true"
        if start:
            endpoint += f"&pk={start}"

        response = requests.get(endpoint, headers=headers)
        if response.status_code == 200:
            data = response.json()
            doc_id = data.get('id')
            if not doc_id:
                break
            print(f"📥 Queued doc_id={doc_id}")
            queue.put(doc_id)
            print("last...")
            start = doc_id + 1

        time.sleep(0.2)  # avoid hammering API

    # Stop workers
    for _ in processes:
        queue.put(None)
    for p in processes:
        p.join()


if __name__ == "__main__":
    process_doc_poster_generate_queue(limit=500, workers=6)
