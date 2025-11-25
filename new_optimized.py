import os
import pathlib
import subprocess
import shutil
import tempfile
import zipfile
import glob
import requests
import time
from PIL import Image
from pdf2image import convert_from_path
from multiprocessing import Process, Queue, cpu_count
from threading import Thread
from lxml import etree as ET

from main_apose import token, BASE_URL

headers = {"Authorization": f"Bearer {token}"}


# ========= Conversion helpers =========

def convert_to_pdf(input_path: str, output_dir: str, timeout: int = 660) -> str:
    """
    Convert Office file → PDF using unoconv.
    Optimized: only convert first few pages if possible (by splitting later).
    """
    input_path = pathlib.Path(input_path)
    output_path = pathlib.Path(output_dir) / f"{input_path.stem}.pdf"

    try:
        subprocess.run([
            "unoconv",
            "-f", "pdf",
            "-o", str(output_path),
            str(input_path)
        ], check=True, timeout=timeout)
        return str(output_path)
    except subprocess.TimeoutExpired as e:
        raise RuntimeError(f"⚠️ Timeout while converting {input_path}") from e
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"❌ Conversion failed: {e}") from e


def not_pdf_to_images_webp_libreoffice(
    ppt_path, output_folder, quality=15, max_width=800, max_slides=4
):
    """
    Convert Office file → PDF → WebP images (limited to first N slides/pages).
    """
    os.makedirs(output_folder, exist_ok=True)
    abs_output = tempfile.mkdtemp(prefix="libreoffice_out_")
    pdf_path = convert_to_pdf(ppt_path, abs_output)

    if not os.path.exists(pdf_path):
        raise RuntimeError(f"❌ PDF was not generated for {ppt_path}")

    print(f"📄 Using generated PDF: {pdf_path}")

    # Only load first N pages to reduce time/memory
    pages = convert_from_path(pdf_path, dpi=150, first_page=1, last_page=max_slides)
    saved_paths = []

    for i, pil_img in enumerate(pages, start=1):
        if pil_img.width > max_width:
            ratio = max_width / pil_img.width
            new_height = int(pil_img.height * ratio)
            pil_img = pil_img.resize((max_width, new_height), Image.LANCZOS)

        webp_path = os.path.join(output_folder, f"slide_{i}.webp")
        pil_img.convert("RGB").save(
            webp_path, "webp",
            quality=quality if i == 1 else 5,
            method=6
        )
        saved_paths.append(webp_path)

    shutil.rmtree(abs_output, ignore_errors=True)

    # total page count (cheap way → use pdfinfo instead of reading full PDF)
    try:
        proc = subprocess.run(
            ["pdfinfo", pdf_path],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True
        )
        pages_count = 0
        for line in proc.stdout.splitlines():
            if line.startswith("Pages:"):
                pages_count = int(line.split(":")[1].strip())
                break
    except Exception:
        pages_count = len(pages)  # fallback

    return saved_paths, pages_count


def pdf_to_images_webp(pdf_path, output_folder, quality=60, max_width=None, max_slides=4):
    os.makedirs(output_folder, exist_ok=True)
    images = convert_from_path(pdf_path, first_page=1, last_page=max_slides, dpi=150)
    saved = []

    for i, img in enumerate(images):
        if max_width and img.width > max_width:
            ratio = max_width / float(img.width)
            new_height = int(float(img.height) * ratio)
            img = img.resize((max_width, new_height))

        img_path = os.path.join(output_folder, f"page_{i+1}.webp")
        img.save(img_path, "WEBP", quality=quality if i == 0 else 5)
        saved.append(img_path)

    # total page count via pdfinfo
    try:
        proc = subprocess.run(
            ["pdfinfo", pdf_path],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True
        )
        pages_count = 0
        for line in proc.stdout.splitlines():
            if line.startswith("Pages:"):
                pages_count = int(line.split(":")[1].strip())
                break
    except Exception:
        pages_count = len(images)

    return saved, pages_count


def download_file(file_url, save_path):
    response = requests.get(file_url)
    response.raise_for_status()
    with open(save_path, "wb") as f:
        f.write(response.content)


# ========= File repair helpers =========
# (unchanged from your code, keeping same functions)

def create_placeholder(path: str, size=(100, 100)):
    img = Image.new("RGB", size, color=(255, 255, 255))
    os.makedirs(os.path.dirname(path), exist_ok=True)
    img.save(path, "PNG")

def clean_xml_references(temp_dir: str, missing_files: list[str]):
    for root_dir, _, files in os.walk(temp_dir):
        for file in files:
            if file.endswith(".xml") or file.endswith(".rels"):
                xml_path = os.path.join(root_dir, file)
                try:
                    tree = ET.parse(xml_path)
                    root = tree.getroot()
                    for blip in root.findall(".//{*}blip"):
                        rid = blip.attrib.get(
                            "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}embed"
                        )
                        if rid and any(m in rid for m in missing_files):
                            parent = blip.getparent()
                            if parent is not None:
                                parent.remove(blip)

                    for rel in root.findall(".//{*}Relationship"):
                        target = rel.attrib.get("Target")
                        if target and any(m in target for m in missing_files):
                            parent = rel.getparent()
                            if parent is not None:
                                parent.remove(rel)

                    tree.write(xml_path)
                except Exception as e:
                    print(f"⚠️ Failed to clean {xml_path}: {e}")


def try_repair_office_file(path: str) -> str | None:
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
        print(f"⚠️ {path} is not a valid Office file, cannot repair.")
        return None

    missing_files = []
    try:
        temp_dir = tempfile.mkdtemp(prefix="repair_office_")
        with zipfile.ZipFile(path, "r") as zip_ref:
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
                        missing_files.append(file)

        if missing_files:
            clean_xml_references(temp_dir, missing_files)

        repaired_path = (
            path.replace(".pptx", "_repaired.pptx")
                .replace(".docx", "_repaired.docx")
        )
        base = repaired_path.replace(".pptx", "").replace(".docx", "")
        shutil.make_archive(base, "zip", temp_dir)

        zip_file = base + ".zip"
        if os.path.exists(zip_file):
            shutil.move(zip_file, repaired_path)

        print(f"🛠️ Repaired file saved: {repaired_path}")
        return repaired_path
    except Exception as e:
        print(f"❌ Repair attempt failed for {path}: {e}")
        return None

# ========= Worker task =========

def generate_docs_for_soff(doc_id):
    temp_path = None
    output_folder = None
    try:
        response = requests.get(
            f"{BASE_URL}/api/v1/seller/admin/product-list/{doc_id}/",
            headers=headers, stream=True
        )
        data = response.json()
        file_type = data["document"]["file_type"].lower()
        file_url = data["document"]["file_url"]
        if not file_url:
            return True

        temp_path = f"temp_copy_{doc_id}{file_type}"
        output_folder = f"images_slide_copy_{doc_id}"
        download_file(file_url, temp_path)

        repaired = None
        if file_type in [".pptx", ".ppt", ".doc", ".docx"] and os.path.exists(temp_path):
            try:
                image_paths, pages_count = not_pdf_to_images_webp_libreoffice(
                    temp_path, output_folder, quality=60, max_width=800
                )
            except Exception as e:
                print(f"⚠️ LibreOffice failed on {temp_path}: {e}")
                repaired = try_repair_office_file(temp_path)
                if repaired:
                    image_paths, pages_count = not_pdf_to_images_webp_libreoffice(
                        repaired, output_folder, quality=60, max_width=800
                    )
                else:
                    return False
            finally:
                if repaired and os.path.exists(repaired):
                    os.remove(repaired)
                if os.path.exists(temp_path):
                    os.remove(temp_path)

        elif file_type == ".pdf":
            image_paths, pages_count = pdf_to_images_webp(
                temp_path, output_folder, quality=60
            )
        else:
            return True

        files = [("images", (os.path.basename(path), open(path, "rb"), "image/webp"))
                 for path in image_paths[:4]]
        data = {"page_count": pages_count}
        response = requests.patch(
            f"{BASE_URL}/api/v1/seller/admin/product-list/{doc_id}/",
            files=files, headers=headers, data=data
        )
        print(f"✅ Finished doc_id={doc_id}, status={response.status_code}")
    except Exception as e:
        print(f"❌ Error doc_id={doc_id}: {e}")
    finally:
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)
        if output_folder and os.path.exists(output_folder):
            shutil.rmtree(output_folder)

    return True


# ========= Worker & Producer =========

def worker(queue: Queue):
    while True:
        doc_id = queue.get()
        if doc_id is None:
            break
        generate_docs_for_soff(doc_id)


def producer(queue: Queue, limit: int, workers: int):
    start = 1
    for _ in range(limit):
        endpoint = f"{BASE_URL}/api/v1/seller/moderation-change/?type=true"
        if start:
            endpoint += f"&pk={start}"

        try:
            response = requests.get(endpoint, headers=headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                doc_id = data.get("id")
                if not doc_id:
                    break
                print(f"📥 Queued doc_id={doc_id}")
                queue.put(doc_id)
                start = doc_id + 1
        except Exception as e:
            print(f"⚠️ Producer error: {e}")
        time.sleep(0.2)

    for _ in range(workers):
        queue.put(None)


def process_doc_poster_generate_queue(limit=100, workers=None):
    workers = workers or max(2, cpu_count() // 2)
    queue = Queue(maxsize=workers * 4)

    processes = [Process(target=worker, args=(queue,)) for _ in range(workers)]
    for p in processes:
        p.start()

    prod_thread = Thread(target=producer, args=(queue, limit, workers))
    prod_thread.start()
    prod_thread.join()

    for p in processes:
        p.join()


if __name__ == "__main__":
    process_doc_poster_generate_queue(limit=1000, workers=6)
