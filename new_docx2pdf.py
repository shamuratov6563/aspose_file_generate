import glob
import os
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
from lxml import etree as ET

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
# Enable/disable PPTX compression (set to "false" to disable)
ENABLE_PPTX_COMPRESSION = os.getenv("ENABLE_PPTX_COMPRESSION", "true").lower() == "true"
# Enable/disable DOCX compression (set to "false" to disable)
ENABLE_DOCX_COMPRESSION = os.getenv("ENABLE_DOCX_COMPRESSION", "true").lower() == "true"
# Enable/disable unoconv (set to "false" to skip unoconv and go straight to other methods)
ENABLE_UNOCONV = os.getenv("ENABLE_UNOCONV", "true").lower() == "true"

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


def compress_pptx_file(pptx_path: str, max_timeout: int = 30) -> str | None:
    """
    Compress a PPTX file using compress-pptx tool with fast settings.
    Returns path to compressed file or None if compression failed.
    The compressed file is saved in a temporary location.
    
    Args:
        pptx_path: Path to the PPTX file to compress
        max_timeout: Maximum time in seconds to wait for compression (default: 30)
    """
    # Check if compression is disabled via environment variable
    if not ENABLE_PPTX_COMPRESSION:
        return None
    
    if not os.path.exists(pptx_path):
        print(f"⚠️ PPTX file not found: {pptx_path}")
        return None
    
    if not pptx_path.lower().endswith('.pptx'):
        print(f"⚠️ File is not a PPTX: {pptx_path}")
        return None
    
    file_size = os.path.getsize(pptx_path)
    file_size_mb = file_size / 1024 / 1024
    
    # Skip compression for files smaller than 3MB or larger than 50MB
    # Small files: compression overhead not worth it
    # Very large files: compression takes too long
    if file_size < 3 * 1024 * 1024:  # 3MB
        print(f"⏭️ Skipping compression for small file ({file_size_mb:.1f}MB)")
        return None
    if file_size > 50 * 1024 * 1024:  # 50MB
        print(f"⏭️ Skipping compression for large file ({file_size_mb:.1f}MB) - too slow")
        return None
    
    try:
        # Create a temporary compressed file
        compressed_path = pptx_path.replace('.pptx', '_compressed.pptx')
        
        # Get the path to compress-pptx binary (try venv first, then system)
        compress_pptx_bin = None
        venv_bin = os.path.join(os.path.dirname(__file__), 'venv', 'bin', 'compress-pptx')
        if os.path.exists(venv_bin):
            compress_pptx_bin = venv_bin
        else:
            # Try system-wide
            result = subprocess.run(['which', 'compress-pptx'], 
                                  capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                compress_pptx_bin = result.stdout.strip()
        
        if not compress_pptx_bin:
            print(f"⚠️ compress-pptx not found, skipping compression for {pptx_path}")
            return None
        
        print(f"🗜️ Compressing PPTX ({file_size_mb:.1f}MB): {pptx_path}")
        
        # Use aggressive timeout: 5 seconds per MB, max 30 seconds
        # For 5MB file: 25 seconds max
        timeout = min(int(file_size_mb * 5), max_timeout)
        
        # Use faster compression settings:
        # - Lower JPEG quality (75 instead of 85) for faster processing
        # - Skip JPEG recompression (already compressed)
        # - Only compress images larger than 500KB (faster)
        result = subprocess.run(
            [
                compress_pptx_bin, 
                pptx_path, 
                '-o', compressed_path, 
                '-f',  # Force overwrite
                '-q', '75',  # Lower quality for speed
                '-s', '500k',  # Only compress images > 500KB
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout
        )
        
        if result.returncode != 0:
            print(f"⚠️ Compression failed for {pptx_path}: {result.stderr[:200]}")
            if os.path.exists(compressed_path):
                os.remove(compressed_path)
            return None
        
        if os.path.exists(compressed_path):
            original_size = os.path.getsize(pptx_path)
            compressed_size = os.path.getsize(compressed_path)
            
            # Only use compressed file if it's at least 3% smaller
            reduction = ((original_size - compressed_size) / original_size) * 100
            if reduction < 3:
                print(f"⏭️ Compression not effective ({reduction:.1f}% reduction), using original file")
                os.remove(compressed_path)
                return None
            
            print(f"✅ Compressed PPTX: {original_size:,} → {compressed_size:,} bytes ({reduction:.1f}% reduction)")
            return compressed_path
        else:
            print(f"⚠️ Compressed file not created: {compressed_path}")
            return None
            
    except subprocess.TimeoutExpired:
        print(f"⚠️ Compression timeout ({timeout}s) for {pptx_path}, using original file")
        if 'compressed_path' in locals() and os.path.exists(compressed_path):
            os.remove(compressed_path)
        return None
    except Exception as e:
        print(f"⚠️ Compression error for {pptx_path}: {e}")
        if 'compressed_path' in locals() and os.path.exists(compressed_path):
            os.remove(compressed_path)
        return None


def compress_docx_file(docx_path: str, max_timeout: int = 30) -> str | None:
    """
    Compress a DOCX file by compressing images within it.
    Returns path to compressed file or None if compression failed.
    The compressed file is saved in a temporary location.
    
    Args:
        docx_path: Path to the DOCX file to compress
        max_timeout: Maximum time in seconds to wait for compression (default: 30)
    """
    # Check if compression is disabled via environment variable
    if not ENABLE_DOCX_COMPRESSION:
        return None
    
    if not os.path.exists(docx_path):
        print(f"⚠️ DOCX file not found: {docx_path}")
        return None
    
    if not docx_path.lower().endswith('.docx'):
        print(f"⚠️ File is not a DOCX: {docx_path}")
        return None
    
    file_size = os.path.getsize(docx_path)
    file_size_mb = file_size / 1024 / 1024
    
    # Skip compression for files smaller than 3MB or larger than 50MB
    # Small files: compression overhead not worth it
    # Very large files: compression takes too long
    if file_size < 3 * 1024 * 1024:  # 3MB
        print(f"⏭️ Skipping compression for small file ({file_size_mb:.1f}MB)")
        return None
    if file_size > 50 * 1024 * 1024:  # 50MB
        print(f"⏭️ Skipping compression for large file ({file_size_mb:.1f}MB) - too slow")
        return None
    
    temp_dir = None
    try:
        start_time = time.time()
        print(f"🗜️ Compressing DOCX ({file_size_mb:.1f}MB): {docx_path}")
        
        # Create a temporary directory for extraction
        temp_dir = tempfile.mkdtemp(prefix="compress_docx_")
        compressed_path = docx_path.replace('.docx', '_compressed.docx')
        
        # Extract DOCX (it's a ZIP file)
        with zipfile.ZipFile(docx_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        
        # Find and compress images in the media folder
        media_dir = os.path.join(temp_dir, 'word', 'media')
        images_compressed = 0
        total_saved = 0
        
        if os.path.exists(media_dir):
            for filename in os.listdir(media_dir):
                # Check timeout
                if time.time() - start_time > max_timeout:
                    print(f"⚠️ Compression timeout ({max_timeout}s) for {docx_path}")
                    shutil.rmtree(temp_dir, ignore_errors=True)
                    return None
                
                file_path = os.path.join(media_dir, filename)
                if not os.path.isfile(file_path):
                    continue
                
                # Only process image files
                if not filename.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp', '.gif')):
                    continue
                
                file_size_before = os.path.getsize(file_path)
                
                # Lower threshold for compression - compress images > 100KB
                # Many small images can add up, especially in documents with many diagrams
                if file_size_before < 100 * 1024:  # 100KB
                    continue
                
                try:
                    # Open and compress image
                    original_ext = os.path.splitext(filename)[1].lower()
                    temp_img_path = file_path + '.tmp'
                    
                    with Image.open(file_path) as img:
                        if original_ext in ('.jpg', '.jpeg'):
                            # Compress JPEG files
                            # Convert to RGB if necessary
                            if img.mode in ('RGBA', 'LA', 'P'):
                                # Create white background for transparency
                                background = Image.new('RGB', img.size, (255, 255, 255))
                                if img.mode == 'P':
                                    img = img.convert('RGBA')
                                background.paste(img, mask=img.split()[-1] if img.mode in ('RGBA', 'LA') else None)
                                img = background
                            elif img.mode != 'RGB':
                                img = img.convert('RGB')
                            
                            # Recompress JPEG with lower quality
                            img.save(temp_img_path, 'JPEG', quality=75, optimize=True)
                            
                        elif original_ext == '.png':
                            # Optimize PNG files in place (keep same format to avoid XML issues)
                            # PNG optimization can significantly reduce file size
                            img.save(temp_img_path, 'PNG', optimize=True, compress_level=9)
                            
                        else:
                            # Skip BMP/GIF - conversion would require XML updates
                            # Focus on JPEG and PNG which are most common
                            continue
                        
                        # Only replace if smaller (at least 5% reduction to be worth it)
                        file_size_after = os.path.getsize(temp_img_path)
                        reduction_percent = ((file_size_before - file_size_after) / file_size_before) * 100
                        
                        if file_size_after < file_size_before and reduction_percent >= 5:
                            os.replace(temp_img_path, file_path)
                            saved = file_size_before - file_size_after
                            total_saved += saved
                            images_compressed += 1
                        else:
                            os.remove(temp_img_path)
                            
                except Exception as e:
                    print(f"⚠️ Failed to compress image {filename}: {e}")
                    if 'temp_img_path' in locals() and os.path.exists(temp_img_path):
                        os.remove(temp_img_path)
                    continue
        
        # Rebuild DOCX file
        with zipfile.ZipFile(compressed_path, 'w', zipfile.ZIP_DEFLATED) as zip_out:
            for root, dirs, files in os.walk(temp_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, temp_dir)
                    zip_out.write(file_path, arcname)
        
        # Cleanup temp directory
        shutil.rmtree(temp_dir, ignore_errors=True)
        temp_dir = None
        
        if os.path.exists(compressed_path):
            original_size = os.path.getsize(docx_path)
            compressed_size = os.path.getsize(compressed_path)
            
            # Only use compressed file if it's at least 3% smaller
            reduction = ((original_size - compressed_size) / original_size) * 100
            if reduction < 3:
                print(f"⏭️ Compression not effective ({reduction:.1f}% reduction), using original file")
                os.remove(compressed_path)
                return None
            
            elapsed = time.time() - start_time
            print(f"✅ Compressed DOCX: {original_size:,} → {compressed_size:,} bytes ({reduction:.1f}% reduction, {images_compressed} images, {elapsed:.1f}s)")
            return compressed_path
        else:
            print(f"⚠️ Compressed file not created: {compressed_path}")
            return None
            
    except Exception as e:
        print(f"⚠️ Compression error for {docx_path}: {e}")
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
        if 'compressed_path' in locals() and os.path.exists(compressed_path):
            os.remove(compressed_path)
        return None


def cleanup_libreoffice_processes():
    """Kill any stuck LibreOffice processes."""
    try:
        # Kill soffice processes
        subprocess.run(['pkill', '-9', '-f', 'soffice'], 
                      timeout=3, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        # Kill unoconv processes
        subprocess.run(['pkill', '-9', 'unoconv'], 
                      timeout=3, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(0.5)  # Give it a moment
    except:
        pass


def docx_to_pdf_unoconv(docx_path: str, output_dir: str, timeout: int = 60) -> str | None:
    """
    Convert DOCX to PDF using unoconv (alternative to LibreOffice direct call).
    Sometimes more stable than direct LibreOffice calls.
    """
    try:
        print(f"  [1/4] Checking if unoconv is available...")
        # Clean up any stuck processes first
        cleanup_libreoffice_processes()
        
        # Check if unoconv is available
        result = subprocess.run(['which', 'unoconv'], 
                              capture_output=True, text=True, timeout=5)
        if result.returncode != 0:
            print(f"  ❌ unoconv not found in PATH")
            return None
        print(f"  ✅ unoconv found: {result.stdout.strip()}")
        
        abs_docx = os.path.abspath(docx_path)
        pdf_path = os.path.join(output_dir, os.path.splitext(os.path.basename(docx_path))[0] + '.pdf')
        
        print(f"  [2/4] Starting unoconv conversion (timeout: {timeout}s)...")
        print(f"  📄 Input: {abs_docx}")
        print(f"  📄 Output: {pdf_path}")
        
        start_time = time.time()
        # Use Popen to allow real-time monitoring
        process = subprocess.Popen([
            'unoconv',
            '-f', 'pdf',
            '-o', pdf_path,
            abs_docx
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        
        # Monitor process with periodic status updates
        check_interval = 10  # Check every 10 seconds
        elapsed = 0
        while process.poll() is None:
            elapsed = time.time() - start_time
            if elapsed > timeout:
                print(f"  ⏱️ Timeout reached ({timeout}s), killing process...")
                try:
                    # Try graceful termination first
                    process.terminate()
                    time.sleep(1)
                    if process.poll() is None:
                        # Force kill if still running
                        print(f"  🔪 Force killing unoconv process (PID: {process.pid})...")
                        process.kill()
                        time.sleep(0.5)
                    
                    # Clean up any related LibreOffice processes
                    cleanup_libreoffice_processes()
                    
                    # Don't wait - just return
                    try:
                        process.wait(timeout=2)
                    except:
                        pass
                except Exception as e:
                    print(f"  ⚠️ Error during cleanup: {e}")
                print(f"  ❌ unoconv timeout after {elapsed:.1f}s - process killed")
                return None
            
            # Print progress every 10 seconds
            if int(elapsed) % check_interval == 0 and int(elapsed) > 0:
                print(f"  ⏳ Still processing... ({int(elapsed)}s elapsed)")
            
            time.sleep(1)
        
        print(f"  [3/4] Process completed in {elapsed:.1f}s, checking result...")
        stdout, stderr = process.communicate()
        
        if process.returncode == 0:
            if os.path.exists(pdf_path):
                file_size = os.path.getsize(pdf_path)
                print(f"  [4/4] ✅ unoconv conversion successful!")
                print(f"  📊 PDF size: {file_size:,} bytes")
                return pdf_path
            else:
                print(f"  ❌ PDF file not created at {pdf_path}")
                if stderr:
                    print(f"  Error: {stderr[:500]}")
                return None
        else:
            print(f"  ❌ unoconv failed with return code {process.returncode}")
            if stderr:
                print(f"  Error output: {stderr[:500]}")
            if stdout:
                print(f"  Stdout: {stdout[:500]}")
            return None
            
    except subprocess.TimeoutExpired:
        print(f"  ❌ unoconv timeout after {timeout}s")
        return None
    except Exception as e:
        print(f"  ❌ unoconv error: {e}")
        import traceback
        print(f"  Traceback: {traceback.format_exc()}")
        return None


def docx_to_pdf_pypandoc(docx_path: str, output_dir: str, timeout: int = 300) -> str | None:
    """
    Convert DOCX to PDF using pypandoc (requires pandoc and LaTeX).
    """
    try:
        import pypandoc
        abs_docx = os.path.abspath(docx_path)
        pdf_path = os.path.join(output_dir, os.path.splitext(os.path.basename(docx_path))[0] + '.pdf')
        
        print(f"🔄 Trying pypandoc for DOCX conversion...")
        # pypandoc uses subprocess internally, so we can't easily timeout it
        # But we can try it
        pypandoc.convert_file(abs_docx, 'pdf', outputfile=pdf_path, format='docx')
        
        if os.path.exists(pdf_path):
            print(f"✅ pypandoc conversion successful: {pdf_path}")
            return pdf_path
        else:
            return None
    except ImportError:
        return None
    except Exception as e:
        print(f"⚠️ pypandoc error: {e}")
        return None


def docx_to_pdf_docx2pdf(docx_path: str, output_dir: str, timeout: int = 300) -> str | None:
    """
    Convert DOCX to PDF using docx2pdf library.
    Uses different backends depending on OS (Word on Windows/Mac, LibreOffice on Linux).
    """
    try:
        from docx2pdf import convert
        abs_docx = os.path.abspath(docx_path)
        pdf_path = os.path.join(output_dir, os.path.splitext(os.path.basename(docx_path))[0] + '.pdf')
        
        print(f"🔄 Trying docx2pdf for DOCX conversion...")
        # docx2pdf doesn't support timeout directly, but we can try
        convert(abs_docx, pdf_path)
        
        if os.path.exists(pdf_path):
            print(f"✅ docx2pdf conversion successful: {pdf_path}")
            return pdf_path
        else:
            return None
    except ImportError:
        return None
    except Exception as e:
        print(f"⚠️ docx2pdf error: {e}")
        return None


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

    # Dynamic timeout based on file size
    file_size_mb = os.path.getsize(abs_ppt) / (1024 * 1024)
    if file_size_mb > 5:
        # Large file: 2 min base + 30s per MB over 5MB, max 10 minutes
        conversion_timeout = min(120 + int((file_size_mb - 5) * 30), 600)
    else:
        # Small file: 2 minutes
        conversion_timeout = 120
    
    pdf_path = None
    is_docx = abs_ppt.lower().endswith('.docx')
    
    # For DOCX files, try alternative methods first if LibreOffice is problematic
    if is_docx:
        print(f"🔄 Attempting DOCX conversion (file size: {file_size_mb:.1f}MB, timeout: {conversion_timeout}s)...")
        print(f"📋 Will try methods in order: unoconv → docx2pdf → pypandoc → LibreOffice")
        
        # Try alternative methods in order
        # Reduce unoconv timeout to 60s since it's hanging - if it works, it should be fast
        alternatives = []
        if ENABLE_UNOCONV:
            alternatives.append(('unoconv', lambda: docx_to_pdf_unoconv(abs_ppt, abs_output, min(conversion_timeout, 60))))
        alternatives.extend([
            ('docx2pdf', lambda: docx_to_pdf_docx2pdf(abs_ppt, abs_output, conversion_timeout)),
            ('pypandoc', lambda: docx_to_pdf_pypandoc(abs_ppt, abs_output, conversion_timeout)),
        ])
        
        for idx, (method_name, method_func) in enumerate(alternatives, 1):
            print(f"\n{'='*60}")
            print(f"Method {idx}/{len(alternatives)}: {method_name}")
            print(f"{'='*60}")
            try:
                method_start = time.time()
                pdf_path = method_func()
                method_elapsed = time.time() - method_start
                
                if pdf_path and os.path.exists(pdf_path):
                    print(f"\n✅ Successfully converted using {method_name} (took {method_elapsed:.1f}s)")
                    break
                else:
                    print(f"\n⚠️ {method_name} did not produce a valid PDF, trying next method...")
            except Exception as e:
                print(f"\n❌ {method_name} raised exception: {e}")
                import traceback
                print(traceback.format_exc())
                continue
    
    # Fall back to LibreOffice if alternatives didn't work or for non-DOCX files
    if not pdf_path:
        method_num = (len(alternatives) + 1) if is_docx and 'alternatives' in locals() else 1
        print(f"\n{'='*60}")
        print(f"Method {method_num}: LibreOffice {'(fallback)' if is_docx else ''}")
        print(f"{'='*60}")
        print(f"🔄 Trying LibreOffice conversion...")
        print(f"  [1/3] Creating LibreOffice profile...")
        profile_dir = tempfile.mkdtemp(prefix="libreoffice_profile_")
        print(f"  [2/3] Starting LibreOffice conversion (timeout: {conversion_timeout}s)...")
        
        lo_start = time.time()
        # Use Popen for better monitoring
        process = subprocess.Popen([
            "soffice",
            "--headless",
            "--norestore",
            "--nolockcheck",
            "--nodefault",
            f"-env:UserInstallation=file://{profile_dir}",
            "--convert-to", "pdf",
            "--outdir", abs_output,
            abs_ppt
        ], 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE, 
        text=True,
        )
        
        # Monitor with progress updates
        check_interval = 10
        elapsed = 0
        while process.poll() is None:
            elapsed = time.time() - lo_start
            if elapsed > conversion_timeout:
                print(f"  ⏱️ Timeout reached ({conversion_timeout}s), killing LibreOffice...")
                process.kill()
                process.wait()
                raise subprocess.TimeoutExpired("soffice", conversion_timeout)
            
            if int(elapsed) % check_interval == 0 and int(elapsed) > 0:
                print(f"  ⏳ LibreOffice still processing... ({int(elapsed)}s elapsed)")
            
            time.sleep(1)
        
        stdout, stderr = process.communicate()
        lo_elapsed = time.time() - lo_start
        print(f"  [3/3] LibreOffice completed in {lo_elapsed:.1f}s")
        
        result = type('Result', (), {
            'returncode': process.returncode,
            'stdout': stdout,
            'stderr': stderr
        })()
        
        print("STDOUT:", result.stdout[:1000] if result.stdout else "(empty)")
        print("STDERR:", result.stderr[:1000] if result.stderr else "(empty)")

        if result.returncode != 0:
            # Check if it's a crash (negative return codes indicate signals like SIGSEGV)
            is_crash = result.returncode < 0 or 'segmentation fault' in result.stderr.lower() or 'segfault' in result.stderr.lower()
            error_msg = f"LibreOffice failed: {result.stderr}\n{result.stdout}"
            if is_crash:
                error_msg = f"LibreOffice crashed (returncode={result.returncode}): {result.stderr}\n{result.stdout}"
            raise RuntimeError(error_msg)

        # Find any PDF in the output folder
        pdf_candidates = glob.glob(os.path.join(abs_output, "*.pdf"))
        if not pdf_candidates:
            raise RuntimeError(f"No PDF generated in {abs_output}. LibreOffice stdout: {result.stdout} stderr: {result.stderr}")

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
    if 'profile_dir' in locals():
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
        response = session.get(
            f'{BASE_URL}/api/v1/seller/admin/product-list/{doc_id}/',
            stream=True,
            timeout=REQUEST_TIMEOUT,
        )
        data = response.json()
        file_type = data['document']['file_type'].lower()
        file_url = data['document']['file_url']
        if not file_url:
            return True
        temp_path = f"temp_copy_{doc_id}{file_type}"
        output_folder = f"images_slide_copy_{doc_id}"

        download_file(file_url, temp_path)
        repaired = None
        compressed = None
        if file_type in ['.pptx', '.ppt', '.doc', '.docx'] and os.path.exists(temp_path):
            # Compress PPTX and DOCX files before conversion
            file_to_convert = temp_path
            if file_type == '.pptx':
                compressed = compress_pptx_file(temp_path)
                if compressed and os.path.exists(compressed):
                    file_to_convert = compressed
                    print(f"📦 Using compressed PPTX for conversion: {compressed}")
            elif file_type == '.docx':
                compressed = compress_docx_file(temp_path)
                if compressed and os.path.exists(compressed):
                    file_to_convert = compressed
                    print(f"📦 Using compressed DOCX for conversion: {compressed}")
            
            try:
                image_paths, pages_count = not_pdf_to_images_webp_libreoffice(
                    file_to_convert,
                    output_folder,
                    quality=60,
                    max_width=800,
                )

            except subprocess.TimeoutExpired as e:
                # Handle timeout separately
                error_str = str(e).lower()
                is_timeout = True
                is_crash = False
                
                # If compressed file caused timeout, try original file
                if compressed and file_to_convert == compressed:
                    print(f"⚠️ LibreOffice timed out on compressed file, falling back to original: {temp_path}")
                    try:
                        image_paths, pages_count = not_pdf_to_images_webp_libreoffice(
                            temp_path,
                            output_folder,
                            quality=60,
                            max_width=800,
                        )
                    except Exception as e2:
                        print(f"⚠️ LibreOffice also timed out on original file: {e2}")
                        # Try repair on original file
                        repaired = try_repair_office_file(temp_path)
                        if repaired:
                            image_paths, pages_count = not_pdf_to_images_webp_libreoffice(
                                repaired,
                                output_folder,
                                quality=60,
                                max_width=800,
                            )
                        else:
                            print(f"❌ Skipping doc_id={doc_id}, file processing timeout.")
                            return False
                else:
                    print(f"⚠️ LibreOffice timed out on {file_to_convert}")
                    # Try repair on original file
                    repair_source = temp_path if compressed else file_to_convert
                    repaired = try_repair_office_file(repair_source)
                    if repaired:
                        image_paths, pages_count = not_pdf_to_images_webp_libreoffice(
                            repaired,
                            output_folder,
                            quality=60,
                            max_width=800,
                        )
                    else:
                        print(f"❌ Skipping doc_id={doc_id}, file processing timeout.")
                        return False
            except Exception as e:
                error_str = str(e).lower()
                is_crash = ('segmentation fault' in error_str or 'segfault' in error_str or 
                           'crashed' in error_str or 'crash' in error_str)
                is_timeout = False
                
                # If compressed file caused a crash or timeout, try original file
                if compressed and file_to_convert == compressed and (is_crash or is_timeout):
                    issue_type = "timed out" if is_timeout else "crashed"
                    print(f"⚠️ LibreOffice {issue_type} on compressed file, falling back to original: {temp_path}")
                    try:
                        image_paths, pages_count = not_pdf_to_images_webp_libreoffice(
                            temp_path,
                            output_folder,
                            quality=60,
                            max_width=800,
                        )
                    except Exception as e2:
                        print(f"⚠️ LibreOffice also failed on original file: {e2}")
                        # Try repair on original file
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
                else:
                    print(f"⚠️ LibreOffice failed on {file_to_convert}: {e}")
                    # Try repair on original file if compressed version failed
                    repair_source = temp_path if compressed else file_to_convert
                    repaired = try_repair_office_file(repair_source)
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
                if compressed and os.path.exists(compressed):
                    os.remove(compressed)

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
        if 'compressed' in locals() and compressed and os.path.exists(compressed):
            os.remove(compressed)
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

    start = 710747

    for _ in range(limit):
        endpoint = f"{BASE_URL}/api/v1/seller/moderation-change/?type=true"
        if start:
            endpoint += f"&pk={start}"

        response = session.get(endpoint, timeout=REQUEST_TIMEOUT)
        print(response.status_code, response.text)
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
    process_doc_poster_generate_queue(limit=500, workers=2)
