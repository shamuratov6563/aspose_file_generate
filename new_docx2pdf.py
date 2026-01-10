import glob
import os
import platform
import resource
import shlex
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
LIBREOFFICE_TIMEOUT = int(os.getenv("LIBREOFFICE_TIMEOUT_SECONDS", "180"))  # 3 minutes default
LIBREOFFICE_MEMORY_LIMIT_MB = int(os.getenv("LIBREOFFICE_MEMORY_LIMIT_MB", "1024"))  # 1GB default

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


def count_running_libreoffice_processes():
    """Count how many LibreOffice processes are currently running."""
    try:
        ps_output = subprocess.run(
            ['ps', 'aux'],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=5
        )
        if ps_output.returncode == 0:
            count = sum(1 for line in ps_output.stdout.split('\n') 
                       if 'soffice' in line.lower() and 'grep' not in line)
            return count
    except Exception:
        pass
    return 0


def kill_all_libreoffice_processes(profile_dir: str, process_pid: int = None, timeout: int = 10):
    """
    Kill ALL LibreOffice processes related to a specific profile directory.
    This includes soffice.bin, soffice processes, and any child processes.
    
    Args:
        profile_dir: The LibreOffice profile directory to identify related processes
        process_pid: Optional parent process PID to kill first
        timeout: Timeout for killing processes
    """
    killed_pids = []
    
    try:
        # First, try to kill the parent process if provided
        if process_pid:
            try:
                os.kill(process_pid, 15)  # SIGTERM
                time.sleep(1)
                if subprocess.run(['ps', '-p', str(process_pid)], 
                                 stdout=subprocess.DEVNULL, 
                                 stderr=subprocess.DEVNULL).returncode == 0:
                    os.kill(process_pid, 9)  # SIGKILL
                killed_pids.append(process_pid)
            except (ProcessLookupError, OSError):
                pass  # Process already dead
        
        # Extract profile name from full path for matching
        profile_name = os.path.basename(profile_dir)
        if not profile_name:
            profile_name = os.path.basename(os.path.dirname(profile_dir))
        
        # Find and kill all soffice/soffice.bin processes with this profile
        try:
            # Use ps to find processes containing the profile path
            ps_output = subprocess.run(
                ['ps', 'aux'],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                timeout=5
            )
            
            if ps_output.returncode == 0:
                for line in ps_output.stdout.split('\n'):
                    if profile_name in line and ('soffice' in line or 'Xvfb' in line):
                        parts = line.split()
                        if len(parts) >= 2:
                            try:
                                pid = int(parts[1])
                                if pid not in killed_pids and pid != os.getpid():
                                    # Kill the process
                                    try:
                                        os.kill(pid, 15)  # SIGTERM first
                                        killed_pids.append(pid)
                                    except (ProcessLookupError, OSError):
                                        pass
                            except (ValueError, IndexError):
                                pass
        except Exception as e:
            print(f"⚠️  Error finding processes: {e}")
        
        # Wait a bit, then force kill any remaining
        time.sleep(2)
        for pid in killed_pids:
            try:
                if subprocess.run(['ps', '-p', str(pid)], 
                                stdout=subprocess.DEVNULL, 
                                stderr=subprocess.DEVNULL).returncode == 0:
                    os.kill(pid, 9)  # SIGKILL
            except (ProcessLookupError, OSError):
                pass
        
        # Also use pkill as fallback for any remaining soffice.bin processes
        try:
            subprocess.run(
                ['pkill', '-f', f'libreoffice_profile.*{profile_name}'],
                timeout=3,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
        except Exception:
            pass
        
        if killed_pids:
            print(f"🧹 Killed {len(killed_pids)} LibreOffice process(es): {killed_pids}")
            
    except Exception as e:
        print(f"⚠️  Error in kill_all_libreoffice_processes: {e}")


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
    file_name = os.path.basename(ppt_path)
    file_size_mb = os.path.getsize(ppt_path) / (1024 * 1024)
    
    # Calculate dynamic timeout based on file size (minimum 120s, add 15s per MB)
    # Large files need more time to convert
    dynamic_timeout = max(120, LIBREOFFICE_TIMEOUT + int(file_size_mb * 15))
    if dynamic_timeout > LIBREOFFICE_TIMEOUT:
        print(f"\n{'='*80}")
        print(f"🔄 Starting conversion: {file_name}")
        print(f"📊 File size: {file_size_mb:.2f}MB | Memory limit: {LIBREOFFICE_MEMORY_LIMIT_MB}MB")
        print(f"⏱️  Timeout adjusted: {LIBREOFFICE_TIMEOUT}s → {dynamic_timeout}s (based on file size)")
        print(f"{'='*80}")
    else:
        print(f"\n{'='*80}")
        print(f"🔄 Starting conversion: {file_name}")
        print(f"📊 File size: {file_size_mb:.2f}MB | Memory limit: {LIBREOFFICE_MEMORY_LIMIT_MB}MB | Timeout: {LIBREOFFICE_TIMEOUT}s")
        print(f"{'='*80}")
        dynamic_timeout = LIBREOFFICE_TIMEOUT
    
    abs_ppt = os.path.abspath(ppt_path)
    abs_output = tempfile.mkdtemp(prefix="libreoffice_out_")
    os.makedirs(output_folder, exist_ok=True)
    print(f"📁 Created output directory: {abs_output}")
    print(f"📁 Created image folder: {output_folder}")

    profile_dir = tempfile.mkdtemp(prefix="libreoffice_profile_")
    print(f"📁 Created LibreOffice profile: {profile_dir}")

    # Configure LibreOffice profile to disable Java
    print(f"⚙️  Configuring LibreOffice profile...")
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

    # Check if xvfb is needed (only for Linux systems without display)
    # On macOS, LibreOffice works in headless mode without xvfb
    use_xvfb = False
    if platform.system() == 'Linux':
        # On Linux, xvfb may be needed if no display is available
        use_xvfb = check_xvfb_available()
        if use_xvfb:
            # Wrap command with xvfb-run for virtual display and ulimit for memory
            # -a: auto-display number, -s: server args, screen 0: virtual screen
            memory_limit_kb = LIBREOFFICE_MEMORY_LIMIT_MB * 1024
            soffice_cmd_escaped = ' '.join(shlex.quote(arg) for arg in soffice_cmd)
            soffice_cmd = [
                "bash", "-c",
                f"ulimit -v {memory_limit_kb} && xvfb-run -a -s '-screen 0 1024x768x24' {soffice_cmd_escaped}"
            ]
            print(f"ℹ️ Using xvfb-run for virtual display with memory limit: {LIBREOFFICE_MEMORY_LIMIT_MB}MB")
    # On macOS, LibreOffice works in headless mode without needing xvfb

    # Monitor running LibreOffice processes before starting
    running_count = count_running_libreoffice_processes()
    if running_count > 0:
        print(f"📊 Currently running LibreOffice processes: {running_count}")
    
    print(f"📊 LibreOffice memory limit: {LIBREOFFICE_MEMORY_LIMIT_MB}MB, timeout: {dynamic_timeout}s")
    print(f"🚀 Launching LibreOffice process...")
    
    process_start_time = time.time()
    process = None
    process_pid = None
    
    try:
        # Use Popen to monitor process in real-time
        process = subprocess.Popen(
            soffice_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            preexec_fn=set_process_limits if not use_xvfb else None,
            env=env
        )
        process_pid = process.pid
        print(f"✅ LibreOffice process started | PID: {process_pid}")
        
        # Monitor process with periodic updates
        check_interval = 5  # Check every 5 seconds
        last_check = 0
        
        while process.poll() is None:
            elapsed = time.time() - process_start_time
            
            # Print progress every check_interval seconds
            if int(elapsed) >= last_check + check_interval:
                print(f"⏳ Processing... ({int(elapsed)}s elapsed)")
                last_check = int(elapsed)
            
            # Check for timeout
            if elapsed > dynamic_timeout:
                print(f"\n⏱️  TIMEOUT REACHED ({dynamic_timeout}s)")
                print(f"🔪 Killing ALL LibreOffice processes for profile...")
                
                # Use the robust cleanup function to kill all related processes
                kill_all_libreoffice_processes(profile_dir, process_pid)
                
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    print(f"⚠️  Process did not terminate within wait timeout")
                
                # Clean up temp directories
                print(f"🧹 Cleaning up temporary directories...")
                if os.path.exists(abs_output):
                    shutil.rmtree(abs_output, ignore_errors=True)
                    print(f"🗑️  Deleted: {abs_output}")
                if os.path.exists(profile_dir):
                    shutil.rmtree(profile_dir, ignore_errors=True)
                    print(f"🗑️  Deleted: {profile_dir}")
                
                raise subprocess.TimeoutExpired("soffice", dynamic_timeout)
            
            time.sleep(1)
        
        # Process completed
        elapsed_total = time.time() - process_start_time
        print(f"✅ Process completed in {elapsed_total:.1f}s | Exit code: {process.returncode}")
        
        # Get stdout and stderr
        stdout, stderr = process.communicate()
        
        if stdout:
            print(f"📤 STDOUT: {stdout[:500]}{'...' if len(stdout) > 500 else ''}")
        if stderr:
            print(f"📤 STDERR: {stderr[:500]}{'...' if len(stderr) > 500 else ''}")
        
        # Check for memory limit violations (signal 9 = SIGKILL, signal 11 = SIGSEGV)
        if process.returncode == -9 or (stderr and ('Killed' in stderr or 'SIGKILL' in stderr)):
            print(f"\n⚠️  MEMORY LIMIT VIOLATION DETECTED!")
            print(f"💀 Process was killed due to exceeding {LIBREOFFICE_MEMORY_LIMIT_MB}MB memory limit")
            print(f"🧹 Cleaning up temporary files...")
            if os.path.exists(abs_output):
                shutil.rmtree(abs_output, ignore_errors=True)
                print(f"🗑️  Deleted: {abs_output}")
            if os.path.exists(profile_dir):
                shutil.rmtree(profile_dir, ignore_errors=True)
                print(f"🗑️  Deleted: {profile_dir}")
            raise RuntimeError(f"LibreOffice exceeded memory limit ({LIBREOFFICE_MEMORY_LIMIT_MB}MB) and was killed")
        
        result = type('Result', (), {
            'returncode': process.returncode,
            'stdout': stdout,
            'stderr': stderr
        })()
        
    except subprocess.TimeoutExpired as e:
        print(f"\n❌ Conversion failed: Timeout after {dynamic_timeout}s")
        # Ensure cleanup happens
        if process_pid:
            kill_all_libreoffice_processes(profile_dir, process_pid)
        raise
    except Exception as e:
        if process and process.poll() is None:
            print(f"\n⚠️  Exception occurred, cleaning up process (PID: {process_pid})...")
            kill_all_libreoffice_processes(profile_dir, process_pid)
        raise
    finally:
        # Always ensure cleanup of any remaining processes
        if process_pid and process and process.poll() is None:
            try:
                kill_all_libreoffice_processes(profile_dir, process_pid)
            except:
                pass

    # Find any PDF in the output folder
    print(f"\n📋 Searching for generated PDF in: {abs_output}")
    pdf_candidates = glob.glob(os.path.join(abs_output, "*.pdf"))
    
    if pdf_candidates:
        print(f"✅ Found {len(pdf_candidates)} PDF file(s)")
        for pdf in pdf_candidates:
            pdf_size = os.path.getsize(pdf) / (1024 * 1024)
            print(f"   📄 {os.path.basename(pdf)} ({pdf_size:.2f}MB)")

    # Check if PDF was actually created, even if returncode is non-zero
    # (Java warnings can cause non-zero exit codes even when conversion succeeds)
    if not pdf_candidates:
        error_msg = result.stderr or result.stdout or "Unknown error"
        print(f"\n❌ No PDF file generated!")
        print(f"🧹 Cleaning up temporary directories...")
        
        # Check for X11/display errors
        if "X11 error" in error_msg or "Can't open display" in error_msg or "DISPLAY" in error_msg:
            xvfb_available = check_xvfb_available()
            if not xvfb_available:
                if os.path.exists(abs_output):
                    shutil.rmtree(abs_output, ignore_errors=True)
                    print(f"🗑️  Deleted: {abs_output}")
                if os.path.exists(profile_dir):
                    shutil.rmtree(profile_dir, ignore_errors=True)
                    print(f"🗑️  Deleted: {profile_dir}")
                raise RuntimeError(
                    f"LibreOffice requires a display server. Install xvfb: 'apt-get install xvfb' or 'yum install xorg-x11-server-Xvfb'\n"
                    f"Original error: {error_msg}"
                )
            else:
                if os.path.exists(abs_output):
                    shutil.rmtree(abs_output, ignore_errors=True)
                    print(f"🗑️  Deleted: {abs_output}")
                if os.path.exists(profile_dir):
                    shutil.rmtree(profile_dir, ignore_errors=True)
                    print(f"🗑️  Deleted: {profile_dir}")
                raise RuntimeError(f"LibreOffice X11 error despite xvfb: {error_msg}")
        if result.returncode != 0:
            if os.path.exists(abs_output):
                shutil.rmtree(abs_output, ignore_errors=True)
                print(f"🗑️  Deleted: {abs_output}")
            if os.path.exists(profile_dir):
                shutil.rmtree(profile_dir, ignore_errors=True)
                print(f"🗑️  Deleted: {profile_dir}")
            raise RuntimeError(f"LibreOffice failed: {error_msg}")
        
        if os.path.exists(abs_output):
            shutil.rmtree(abs_output, ignore_errors=True)
            print(f"🗑️  Deleted: {abs_output}")
        if os.path.exists(profile_dir):
            shutil.rmtree(profile_dir, ignore_errors=True)
            print(f"🗑️  Deleted: {profile_dir}")
        raise RuntimeError(
            f"No PDF generated in {abs_output}. LibreOffice stdout: {result.stdout} stderr: {result.stderr}")

    # If PDF was created but returncode is non-zero, log warning but continue
    if result.returncode != 0:
        # Check if stderr only contains Java-related warnings
        stderr_lower = result.stderr.lower()
        java_warnings = ['java', 'javaldx', 'jvm', 'java runtime environment']
        if any(warning in stderr_lower for warning in java_warnings):
            print(f"⚠️  LibreOffice completed conversion but reported Java warnings (PDF was created)")
        else:
            # Non-Java error, but PDF exists - log warning but proceed
            print(f"⚠️  LibreOffice returned non-zero exit code ({result.returncode}) but PDF was created")

    pdf_path = pdf_candidates[0]  # pick first PDF
    pdf_size = os.path.getsize(pdf_path) / (1024 * 1024)
    print(f"\n📄 Using generated PDF: {os.path.basename(pdf_path)} ({pdf_size:.2f}MB)")

    print(f"📊 Extracting page information...")
    total_pages = get_pdf_page_count(pdf_path)
    if total_pages:
        print(f"✅ PDF has {total_pages} pages")
    else:
        print(f"⚠️  Could not determine page count, will extract first {max_slides} pages")
    
    print(f"🖼️  Converting PDF pages to images (max {max_slides} pages)...")
    pages = convert_from_path(
        pdf_path,
        dpi=PDF_DPI,
        first_page=1,
        last_page=max_slides,
    )
    print(f"✅ Extracted {len(pages)} page(s)")
    
    saved_paths = []
    print(f"💾 Saving images as WebP...")

    for i, pil_img in enumerate(pages, start=1):
        original_size = (pil_img.width, pil_img.height)
        if pil_img.width > max_width:
            ratio = max_width / pil_img.width
            new_height = int(pil_img.height * ratio)
            pil_img = pil_img.resize((max_width, new_height), Image.LANCZOS)
            print(f"   📐 Page {i}: Resized {original_size[0]}x{original_size[1]} → {max_width}x{new_height}")

        webp_path = os.path.join(output_folder, f"slide_{i}.webp")
        img_quality = quality if i == 1 else 5
        pil_img.convert("RGB").save(webp_path, "webp", quality=img_quality, method=6)
        webp_size = os.path.getsize(webp_path) / 1024
        saved_paths.append(webp_path)
        print(f"   ✅ Saved: slide_{i}.webp ({webp_size:.1f}KB, quality={img_quality})")

    print(f"\n🧹 Cleaning up temporary files and processes...")
    # Ensure all LibreOffice processes are killed even after successful completion
    if process_pid:
        try:
            kill_all_libreoffice_processes(profile_dir, process_pid)
        except:
            pass
    if os.path.exists(abs_output):
        shutil.rmtree(abs_output, ignore_errors=True)
        print(f"🗑️  Deleted temporary output directory: {abs_output}")
    if os.path.exists(profile_dir):
        shutil.rmtree(profile_dir, ignore_errors=True)
        print(f"🗑️  Deleted LibreOffice profile: {profile_dir}")
    
    print(f"\n{'='*80}")
    print(f"✅ Conversion completed: {file_name}")
    print(f"📊 Result: {len(saved_paths)} images generated | Total pages: {total_pages or len(pages)}")
    print(f"{'='*80}\n")

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

        img_path = os.path.join(output_folder, f"page_{i + 1}.webp")
        img.save(img_path, "WEBP", quality=quality)

    return (
        [os.path.join(output_folder, f"page_{i + 1}.webp") for i in range(len(images))],
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

            # Use xvfb-run only on Linux if available (not needed on macOS)
            use_xvfb = False
            if platform.system() == 'Linux':
                use_xvfb = check_xvfb_available()
                if use_xvfb:
                    # Apply memory limit with xvfb via ulimit
                    memory_limit_kb = LIBREOFFICE_MEMORY_LIMIT_MB * 1024
                    soffice_cmd_escaped = ' '.join(shlex.quote(arg) for arg in soffice_cmd)
                    soffice_cmd = [
                        "bash", "-c",
                        f"ulimit -v {memory_limit_kb} && xvfb-run -a -s '-screen 0 1024x768x24' {soffice_cmd_escaped}"
                    ]

            process = None
            try:
                process = subprocess.run(
                    soffice_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=LIBREOFFICE_TIMEOUT,
                    preexec_fn=set_process_limits if not use_xvfb else None,
                    env=env
                )
                if process.returncode != 0:
                    raise subprocess.CalledProcessError(process.returncode, soffice_cmd, stderr=process.stderr)
            except subprocess.TimeoutExpired:
                # If timeout, kill all related processes
                kill_all_libreoffice_processes(temp_profile)
                raise
            except Exception:
                # On any error, try to cleanup processes
                if process and hasattr(process, 'pid'):
                    kill_all_libreoffice_processes(temp_profile, process.pid)
                raise
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
    try:
        print(f"\n{'#'*80}")
        print(f"🆔 Processing document ID: {doc_id}")
        print(f"{'#'*80}")
        
        response = session.get(
            f'{BASE_URL}/api/v1/seller/admin/product-list/{doc_id}/',
            stream=True,
            timeout=REQUEST_TIMEOUT,
        )
        data = response.json()
        file_type = data['document']['file_type'].lower()
        file_url = data['document']['file_url']
        if not file_url:
            print(f"⚠️  No file URL found for doc_id={doc_id}, skipping")
            return True
        temp_path = f"temp_copy_{doc_id}{file_type}"
        output_folder = f"images_slide_copy_{doc_id}"

        print(f"📥 Downloading file from: {file_url}")
        print(f"💾 Saving to: {temp_path}")
        download_start = time.time()
        download_file(file_url, temp_path)
        download_time = time.time() - download_start
        file_size = os.path.getsize(temp_path) / (1024 * 1024)
        print(f"✅ Download completed in {download_time:.1f}s | File size: {file_size:.2f}MB")
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
                print(f"\n❌ LibreOffice failed on {temp_path}: {e}")
                print(f"🔧 Attempting to repair file...")
                repaired = try_repair_office_file(temp_path)
                if repaired:
                    print(f"✅ File repaired: {repaired}")
                    print(f"🔄 Retrying conversion with repaired file...")
                    try:
                        image_paths, pages_count = not_pdf_to_images_webp_libreoffice(
                            repaired,
                            output_folder,
                            quality=60,
                            max_width=800,
                        )
                        print(f"✅ Successfully processed repaired file: {len(image_paths)} image(s)")
                    except Exception as e2:
                        print(f"❌ Repair attempt also failed: {e2}")
                        print(f"🗑️  Cleaning up files...")
                        if os.path.exists(repaired):
                            os.remove(repaired)
                            print(f"   🗑️  Deleted: {repaired}")
                        if os.path.exists(temp_path):
                            os.remove(temp_path)
                            print(f"   🗑️  Deleted: {temp_path}")
                        if os.path.exists(output_folder):
                            shutil.rmtree(output_folder, ignore_errors=True)
                            print(f"   🗑️  Deleted: {output_folder}")
                            return False
                else:
                    print(f"❌ Could not repair file, skipping doc_id={doc_id}")
                    print(f"🗑️  Cleaning up files...")
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                        print(f"   🗑️  Deleted: {temp_path}")
                    if os.path.exists(output_folder):
                        shutil.rmtree(output_folder, ignore_errors=True)
                        print(f"   🗑️  Deleted: {output_folder}")
                        return False
            finally:
                print(f"\n🧹 Final cleanup of temporary files...")
                if repaired and os.path.exists(repaired):
                    os.remove(repaired)
                    print(f"🗑️  Deleted repaired file: {repaired}")
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                    print(f"🗑️  Deleted original file: {temp_path}")

        elif file_type == '.pdf':
            image_paths, pages_count = pdf_to_images_webp(
                temp_path,
                output_folder,
                quality=60,
            )
        else:
            return True

        # Upload images back
        print(f"\n📤 Uploading {len(image_paths)} image(s) to server...")
        upload_start = time.time()
        with ExitStack() as stack:
            files = []
            for path in image_paths:
                file_size = os.path.getsize(path) / 1024
                print(f"   📤 Uploading: {os.path.basename(path)} ({file_size:.1f}KB)")
                file_handle = stack.enter_context(open(path, "rb"))
                files.append((
                    "images",
                    (os.path.basename(path), file_handle, "image/webp"),
                ))

            data = {'page_count': pages_count}
            print(f"   📊 Page count: {pages_count}")

            session.patch(
                f"{BASE_URL}/api/v1/seller/admin/product-list/{doc_id}/",
                files=files,
                data=data,
                timeout=REQUEST_TIMEOUT,
            )
        upload_time = time.time() - upload_start
        print(f"✅ Upload completed in {upload_time:.1f}s")
        
        # Clean up image folder
        if os.path.exists(output_folder):
            print(f"🧹 Cleaning up image folder: {output_folder}")
            shutil.rmtree(output_folder, ignore_errors=True)
            print(f"🗑️  Deleted: {output_folder}")
        
        print(f"\n{'='*80}")
        print(f"✅ COMPLETED doc_id={doc_id}")
        print(f"{'='*80}\n")

    except Exception as e:
        print(f"\n{'='*80}")
        print(f"❌ ERROR processing doc_id={doc_id}: {e}")
        print(f"{'='*80}")
        import traceback
        print(f"Traceback:\n{traceback.format_exc()}")

    finally:
        # Cleanup temp files
        print(f"\n🧹 Final cleanup for doc_id={doc_id}...")
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)
            print(f"🗑️  Deleted temp file: {temp_path}")
        if output_folder and os.path.exists(output_folder):
            shutil.rmtree(output_folder, ignore_errors=True)
            print(f"🗑️  Deleted output folder: {output_folder}")

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

    start = 895745

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
    process_doc_poster_generate_queue(limit=10000, workers=1)
