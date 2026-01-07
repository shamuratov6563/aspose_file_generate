import glob
import os
import resource
import shutil
import subprocess
import tempfile
import time
import zipfile
from contextlib import ExitStack
from multiprocessing import Process, Queue, cpu_count

import requests
from PIL import Image
from pdf2image import convert_from_path, pdfinfo_from_path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

load_dotenv()  # This loads variables from a .env file in the current directory

# Access environment variables
TOKEN = os.getenv("TOKEN")
BASE_URL = os.getenv("BASE_URL")

headers = {'Authorization': f"Bearer {TOKEN}"}

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "60"))
DOWNLOAD_CHUNK_SIZE = 512 * 1024  # 512 KB chunks for large files
DEFAULT_MAX_SLIDES = 4
DEFAULT_MAX_PDF_PAGES = 3
PDF_DPI = 200

# LibreOffice conversion limits (configurable via environment variables)
LIBREOFFICE_TIMEOUT = int(os.getenv("LIBREOFFICE_TIMEOUT_SECONDS", "120"))  # 10 minutes default
LIBREOFFICE_MEMORY_LIMIT_MB = int(os.getenv("LIBREOFFICE_MEMORY_LIMIT_MB", "2048"))  # 2GB default

session = requests.Session()
session.headers.update(headers)
retry_strategy = Retry(
    total=5,
    backoff_factor=0.5,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=("GET", "PATCH"),
)
adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=16, pool_maxsize=32)
session.mount("https://", adapter)
session.mount("http://", adapter)


def get_pdf_page_count(pdf_path: str) -> int | None:
    """Return the total page count for a PDF, if available."""
    try:
        info = pdfinfo_from_path(pdf_path)
        return int(info.get("Pages", 0))
    except Exception as exc:
        print(f"⚠️ Unable to read PDF info for {pdf_path}: {exc}")
        return None


def set_process_limits():
    """
    Set resource limits for the current process (used as preexec_fn for subprocess).
    Limits memory usage and CPU time for LibreOffice conversion.
    Only works on Unix-like systems (Linux, macOS).
    """
    try:
        # Set memory limit in bytes
        memory_limit_bytes = LIBREOFFICE_MEMORY_LIMIT_MB * 1024 * 1024
        # RLIMIT_AS limits the virtual memory address space
        resource.setrlimit(resource.RLIMIT_AS, (memory_limit_bytes, memory_limit_bytes))
        
        # Set CPU time limit in seconds (soft and hard limit)
        cpu_time_limit = LIBREOFFICE_TIMEOUT
        resource.setrlimit(resource.RLIMIT_CPU, (cpu_time_limit, cpu_time_limit))
        
        # Set data segment size limit (heap memory)
        resource.setrlimit(resource.RLIMIT_DATA, (memory_limit_bytes, memory_limit_bytes))
    except (ValueError, OSError, AttributeError) as e:
        # On some systems, setting limits might fail - log but don't fail
        # AttributeError can occur if resource module doesn't have the constant
        print(f"⚠️ Could not set all resource limits: {e}")


def check_xvfb_available():
    """Check if xvfb-run is available for virtual display."""
    try:
        result = subprocess.run(
            ["which", "xvfb-run"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=120
        )
        return result.returncode == 0
    except Exception:
        return False


def configure_libreoffice_profile(profile_dir: str):
    """
    Configure LibreOffice profile to disable Java and X11 requirements.
    Creates configuration files that tell LibreOffice not to use Java.
    """
    try:
        # Create config directory structure
        config_dir = os.path.join(profile_dir, "user", "config")
        os.makedirs(config_dir, exist_ok=True)
        
        # Create javasettings file to disable Java
        javasettings_file = os.path.join(config_dir, "javasettings_Linux_x86_64.xml")
        javasettings_content = '''<?xml version="1.0" encoding="UTF-8"?>
<oor:component-data xmlns:oor="http://openoffice.org/2001/registry" xmlns:xs="http://www.w3.org/2001/XMLSchema" oor:name="Java" oor:package="org.openoffice.Office">
  <node oor:name="JavaInfo">
    <node oor:name="JavaList">
      <prop oor:name="JavaCount" oor:type="xs:int">
        <value>0</value>
      </prop>
    </node>
  </node>
</oor:component-data>'''
        
        with open(javasettings_file, 'w') as f:
            f.write(javasettings_content)
        
        # Create registrymodifications file to disable Java
        registry_dir = os.path.join(profile_dir, "user", "registrymodifications.xcu")
        if not os.path.exists(registry_dir):
            os.makedirs(os.path.dirname(registry_dir), exist_ok=True)
            registry_content = '''<?xml version="1.0" encoding="UTF-8"?>
<oor:component-data xmlns:oor="http://openoffice.org/2001/registry" xmlns:xs="http://www.w3.org/2001/XMLSchema" oor:name="Common" oor:package="org.openoffice.Office">
  <node oor:name="Misc">
    <node oor:name="FirstRun">
      <prop oor:name="bCompleted" oor:type="xs:boolean">
        <value>true</value>
      </prop>
    </node>
  </node>
</oor:component-data>'''
            with open(registry_dir, 'w') as f:
                f.write(registry_content)
    except Exception as e:
        # If profile configuration fails, continue anyway
        print(f"⚠️ Could not configure LibreOffice profile: {e}")


def not_pdf_to_images_webp_libreoffice(
    ppt_path,
    output_folder,
    quality=15,
    max_width=800,
    max_slides=DEFAULT_MAX_SLIDES,
):
    abs_ppt = os.path.abspath(ppt_path)
    abs_output = tempfile.mkdtemp(prefix="libreoffice_out_")
    os.makedirs(output_folder, exist_ok=True)

    profile_dir = tempfile.mkdtemp(prefix="libreoffice_profile_")
    
    # Configure LibreOffice profile to disable Java
    configure_libreoffice_profile(profile_dir)
    
    # Create environment for headless LibreOffice operation
    env = os.environ.copy()
    # Disable X11/display requirements - unset DISPLAY to prevent X11 errors
    env.pop('DISPLAY', None)
    # Use generic VCL plugin that doesn't require X11
    env['SAL_USE_VCLPLUGIN'] = 'gen'
    env['SAL_DISABLE_OPENCL'] = '1'
    # Disable Java to avoid Java dependency errors
    env.pop('JAVA_HOME', None)
    env.pop('JRE_HOME', None)
    env.pop('JDK_HOME', None)
    
    # Build LibreOffice command
    soffice_cmd = [
        "soffice",
        "--headless",
        "--norestore",
        "--nolockcheck",
        "--nodefault",
        "--nologo",
        f"-env:UserInstallation=file://{profile_dir}",
        "--convert-to", "pdf",
        "--outdir", abs_output,
        abs_ppt
    ]
    
    # Try using xvfb-run if available to provide virtual display
    use_xvfb = check_xvfb_available()
    if use_xvfb:
        # Wrap command with xvfb-run for virtual display
        # -a: auto-display number, -s: server args, screen 0: virtual screen
        soffice_cmd = ["xvfb-run", "-a", "-s", "-screen 0 1024x768x24"] + soffice_cmd
        print("ℹ️ Using xvfb-run for virtual display")
    # If xvfb not available, DISPLAY is unset - LibreOffice should work in headless mode
    # but some versions may still require a display
    
    result = subprocess.run(
        soffice_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=LIBREOFFICE_TIMEOUT,
        preexec_fn=set_process_limits if not use_xvfb else None,  # xvfb-run handles its own process
        env=env
    )

    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)

    # Find any PDF in the output folder
    pdf_candidates = glob.glob(os.path.join(abs_output, "*.pdf"))
    
    # Check if PDF was actually created, even if returncode is non-zero
    # (Java warnings can cause non-zero exit codes even when conversion succeeds)
    if not pdf_candidates:
        error_msg = result.stderr or result.stdout or "Unknown error"
        # Check for X11/display errors
        if "X11 error" in error_msg or "Can't open display" in error_msg or "DISPLAY" in error_msg:
            xvfb_available = check_xvfb_available()
            if not xvfb_available:
                raise RuntimeError(
                    f"LibreOffice requires a display server. Install xvfb: 'apt-get install xvfb' or 'yum install xorg-x11-server-Xvfb'\n"
                    f"Original error: {error_msg}"
                )
            else:
                raise RuntimeError(f"LibreOffice X11 error despite xvfb: {error_msg}")
        if result.returncode != 0:
            raise RuntimeError(f"LibreOffice failed: {error_msg}")
        raise RuntimeError(f"No PDF generated in {abs_output}. LibreOffice stdout: {result.stdout} stderr: {result.stderr}")
    
    # If PDF was created but returncode is non-zero, log warning but continue
    if result.returncode != 0:
        # Check if stderr only contains Java-related warnings
        stderr_lower = result.stderr.lower()
        java_warnings = ['java', 'javaldx', 'jvm', 'java runtime environment']
        if any(warning in stderr_lower for warning in java_warnings):
            print(f"⚠️ LibreOffice completed conversion but reported Java warnings (PDF was created): {result.stderr}")
        else:
            # Non-Java error, but PDF exists - log warning but proceed
            print(f"⚠️ LibreOffice returned non-zero exit code but PDF was created: {result.stderr}")

    pdf_path = pdf_candidates[0]  # pick first PDF
    print(f"📄 Using generated PDF: {pdf_path}")

    total_pages = get_pdf_page_count(pdf_path)
    pages = convert_from_path(
        pdf_path,
        dpi=PDF_DPI,
        first_page=1,
        last_page=max_slides,
    )
    saved_paths = []

    for i, pil_img in enumerate(pages, start=1):
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

    return saved_paths, total_pages or len(pages)


def pdf_to_images_webp(
    pdf_path,
    output_folder,
    quality=60,
    max_width=None,
    max_pages=DEFAULT_MAX_PDF_PAGES,
):
    os.makedirs(output_folder, exist_ok=True)
    total_pages = get_pdf_page_count(pdf_path)
    images = convert_from_path(
        pdf_path,
        first_page=1,
        last_page=max_pages,
        dpi=PDF_DPI,
    )

    for i, img in enumerate(images):
        if max_width and img.width > max_width:
            ratio = max_width / float(img.width)
            new_height = int(float(img.height) * ratio)
            img = img.resize((max_width, new_height))

        img_path = os.path.join(output_folder, f"page_{i+1}.webp")
        img.save(img_path, "WEBP", quality=quality)

    return (
        [os.path.join(output_folder, f"page_{i+1}.webp") for i in range(len(images))],
        total_pages or len(images),
    )


def download_file(file_url, save_path):
    with session.get(file_url, stream=True, timeout=REQUEST_TIMEOUT) as response:
        response.raise_for_status()
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                if chunk:
                    f.write(chunk)


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
            # Create temporary profile for this conversion
            temp_profile = tempfile.mkdtemp(prefix="libreoffice_profile_")
            configure_libreoffice_profile(temp_profile)
            
            # Create environment for headless LibreOffice operation
            env = os.environ.copy()
            env.pop('DISPLAY', None)
            env['SAL_USE_VCLPLUGIN'] = 'gen'
            env['SAL_DISABLE_OPENCL'] = '1'
            env.pop('JAVA_HOME', None)
            env.pop('JRE_HOME', None)
            env.pop('JDK_HOME', None)
            
            # Build command
            soffice_cmd = [
                "soffice",
                "--headless",
                "--nologo",
                f"-env:UserInstallation=file://{temp_profile}",
                "--convert-to", "pptx",
                path
            ]
            
            # Use xvfb-run if available
            use_xvfb = check_xvfb_available()
            if use_xvfb:
                soffice_cmd = ["xvfb-run", "-a", "-s", "-screen 0 1024x768x24"] + soffice_cmd
            
            subprocess.run(
                soffice_cmd,
                check=True,
                timeout=LIBREOFFICE_TIMEOUT,
                preexec_fn=set_process_limits if not use_xvfb else None,
                env=env
            )
            # Cleanup temp profile
            shutil.rmtree(temp_profile, ignore_errors=True)
            
            if os.path.exists(pptx_path):
                print(f"🌀 Converted old PPT → PPTX: {pptx_path}")
                path = pptx_path
            else:
                print(f"❌ Failed to convert {path} to PPTX")
                return None
        except Exception as e:
            print(f"❌ LibreOffice conversion failed for {path}: {e}")
            # Cleanup temp profile on error
            if 'temp_profile' in locals():
                shutil.rmtree(temp_profile, ignore_errors=True)
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
    success = False
    try:
        response = session.get(
            f'{BASE_URL}/api/v1/seller/admin/product-list/{doc_id}/',
            stream=True,
            timeout=REQUEST_TIMEOUT,
        )
        data = response.json()
        file_type = data['document']['file_type'].lower()
        file_url = data['document'].get('file_url')  # Keep file_url for S3 fallback

        # Ensure file_type has a dot prefix if not present
        if file_type and not file_type.startswith('.'):
            file_type = f'.{file_type}'

        if not file_type:
            print(f"⚠️ No file_type found for doc_id={doc_id}")
            return False

        # Prepare temp path and output folder
        temp_path = f"temp_copy_{doc_id}{file_type}"
        output_folder = f"images_slide_copy_{doc_id}"

        # Try local file first (cost-effective)
        # Path format: /root/FileConversionBackend/media/uploaded_files/{doc_id}.{extension}
        local_file_path = f"/root/FileConversionBackend/media/uploaded_files/{doc_id}{file_type}"

        if os.path.exists(local_file_path):
            print(f"📁 Using local file for doc_id={doc_id}: {local_file_path}")
            # Copy to temp location for processing
            shutil.copy2(local_file_path, temp_path)
        elif file_url:
            # Fallback to S3 if local file doesn't exist
            print(f"☁️ Local file not found, downloading from S3 for doc_id={doc_id}")
            download_file(file_url, temp_path)
        else:
            print(f"⚠️ No file found locally and no file_url available for doc_id={doc_id}")
            return False
        repaired = None
        if file_type in ['.pptx', '.ppt', '.doc', '.docx'] and os.path.exists(temp_path):
            try:
                image_paths, pages_count = not_pdf_to_images_webp_libreoffice(
                    temp_path,
                    output_folder,
                    quality=60,
                    max_width=800,
                )

            except Exception as e:
                print(f"⚠️ LibreOffice failed on {temp_path}: {e}")
                repaired = try_repair_office_file(temp_path)
                if repaired:
                    image_paths, pages_count = not_pdf_to_images_webp_libreoffice(
                        repaired,
                        output_folder,
                        quality=60,
                        max_width=800,
                    )
                else:
                    print(f"❌ Skipping doc_id={doc_id}, corrupted file.")
                    return False
            finally:
                if repaired and os.path.exists(repaired):
                    os.remove(repaired)
                if os.path.exists(temp_path):
                    os.remove(temp_path)

        elif file_type == '.pdf':
            image_paths, pages_count = pdf_to_images_webp(
                temp_path,
                output_folder,
                quality=60,
            )
        else:
            return True

        # Upload images back
        with ExitStack() as stack:
            files = []
            for path in image_paths:
                file_handle = stack.enter_context(open(path, "rb"))
                files.append((
                    "images",
                    (os.path.basename(path), file_handle, "image/webp"),
                ))

            data = {'page_count': pages_count}

            session.patch(
                f"{BASE_URL}/api/v1/seller/admin/product-list/{doc_id}/",
                files=files,
                data=data,
                timeout=REQUEST_TIMEOUT,
            )
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

    start = 1

    for _ in range(limit):
        endpoint = f"{BASE_URL}/api/v1/seller/moderation-change/?type=true"
        if start:
            endpoint += f"&pk={start}"

        response = session.get(endpoint, timeout=REQUEST_TIMEOUT)
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
    process_doc_poster_generate_queue(limit=100, workers=4)
