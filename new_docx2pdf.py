import glob
import os
import resource
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from contextlib import ExitStack
from multiprocessing import Process, Queue, cpu_count
from queue import Empty

import requests
from PIL import Image
from pdf2image import convert_from_path, pdfinfo_from_path
import inspect
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

load_dotenv()  # This loads variables from a .env file in the current directory


def env_int(name: str, default: int) -> int:
    """
    Read an integer setting from the environment.

    A blank or malformed value falls back to the default instead of killing the
    whole run at import time (a stray `LIBREOFFICE_TIMEOUT_SECONDS=` in .env
    used to crash the process before a single document was touched).
    """
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw.strip())
    except ValueError:
        print(f"WARNING: {name}={raw!r} is not an integer, using default {default}")
        return default


def env_float(name: str, default: float) -> float:
    """Same as env_int, for fractional settings like a retry backoff factor."""
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw.strip())
    except ValueError:
        print(f"WARNING: {name}={raw!r} is not a number, using default {default}")
        return default


def env_bool(name: str, default: bool) -> bool:
    """Accepts 1/0, true/false, yes/no, on/off (case-insensitive)."""
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    value = raw.strip().lower()
    if value in ("1", "true", "yes", "on"):
        return True
    if value in ("0", "false", "no", "off"):
        return False
    print(f"WARNING: {name}={raw!r} is not a boolean, using default {default}")
    return default

def env_str(name: str, default: str = "") -> str:
    raw = os.getenv(name)
    return raw.strip() if raw and raw.strip() else default


# Access environment variables
BASE_URL = os.getenv("BASE_URL")
# Shared secret for the poster-generation service API; must match
# POSTER_SERVICE_SECRET_KEY on the Soff server. This replaces the old superuser
# Bearer token: the service endpoints only expose moderation-queue documents and
# only accept poster images, so a leaked key here cannot edit prices, approve
# products or read seller data the way the admin token could.
SECRET_KEY = env_str("SECRET_KEY")
# Legacy: only still used if SERVICE_API=0 pins the run to the old admin API.
TOKEN = os.getenv("TOKEN")
USE_SERVICE_API = env_bool("SERVICE_API", True)
LIMIT = env_int("LIMIT", 10000)
START_ID = env_int("START_ID", 1195807)

# ---- Service queue ----
# How many jobs to pull per queue request. One call now carries file_url for
# every document, so the old "one GET for the next id + one GET for its
# metadata, per document" pattern collapses to a single request per batch.
QUEUE_BATCH_SIZE = env_int("QUEUE_BATCH_SIZE", 50)
# Walk order asked of the server:
#   fair   -- sellers with the fewest documents still waiting first, oldest
#             first within a seller, so one bulk uploader cannot block everyone
#   oldest -- plain oldest upload first
#   newest -- newest upload first
# Empty leaves the choice to the server's POSTER_QUEUE_ORDER setting.
QUEUE_ORDER = env_str("QUEUE_ORDER", "fair").lower()
VALID_QUEUE_ORDERS = ("", "fair", "oldest", "newest")
if QUEUE_ORDER not in VALID_QUEUE_ORDERS:
    print(f"WARNING: QUEUE_ORDER={QUEUE_ORDER!r} is not one of"
          f" {VALID_QUEUE_ORDERS}, falling back to 'fair'")
    QUEUE_ORDER = "fair"
# A document the worker keeps failing on stays in the queue (it never gets a
# poster), so without this the producer would hand out the same bad id forever.
# Failed ids are sent back as `exclude_ids` and, past that cap, stepped over
# with `offset`.
QUEUE_MAX_EXCLUDE_IDS = env_int("QUEUE_MAX_EXCLUDE_IDS", 500)

# Define a local temporary directory to bypass Snap sandboxing which often prevents access to system /tmp
LOCAL_TMP_DIR = os.path.join(os.getcwd(), 'tmp_libreoffice')
os.makedirs(LOCAL_TMP_DIR, exist_ok=True)

# Documents that failed conversion are appended here so they can be re-run.
# Without this a failed doc is silently skipped and stays stuck in moderation forever.
FAILED_LOG = os.getenv("FAILED_LOG", os.path.join(os.getcwd(), "failed_doc_ids.log"))

if USE_SERVICE_API:
    if not SECRET_KEY:
        raise SystemExit(
            "SECRET_KEY is required: it is the X-Secret-Key for the poster service"
            " API, and must match POSTER_SERVICE_SECRET_KEY on the Soff server (not"
            " its SECURE_KEY). Set SERVICE_API=0 to fall back to the old admin API"
            " with TOKEN."
        )
    headers = {"X-Secret-Key": SECRET_KEY}
else:
    if not TOKEN:
        raise SystemExit("SERVICE_API=0 needs TOKEN (admin Bearer token)")
    headers = {"Authorization": f"Bearer {TOKEN}"}

# Service API (secret key, moderation queue only)
QUEUE_URL = f"{BASE_URL}/api/v1/seller/service/poster/queue/"
JOB_URL = f"{BASE_URL}/api/v1/seller/service/poster/document/{{doc_id}}/"
UPLOAD_URL = f"{BASE_URL}/api/v1/seller/service/poster/upload/"
# Legacy admin API (superuser token, shared with the admin panel)
LEGACY_NEXT_URL = f"{BASE_URL}/api/v1/seller/moderation-change/"
LEGACY_DETAIL_URL = f"{BASE_URL}/api/v1/seller/admin/product-list/{{doc_id}}/"

REQUEST_TIMEOUT = env_int("REQUEST_TIMEOUT_SECONDS", 120)
DOWNLOAD_CHUNK_SIZE = 512 * 1024  # 512 KB chunks for large files
DEFAULT_MAX_SLIDES = 4
DEFAULT_MAX_PDF_PAGES = 3
PDF_DPI = 300

# pdf2image only grew a `timeout` kwarg in later versions; pass it when the
# installed version accepts it so poppler cannot outlive the document budget.
_CONVERT_TIMEOUT_SUPPORTED = "timeout" in inspect.signature(convert_from_path).parameters


def raster_kwargs(deadline, stage):
    """convert_from_path kwargs carrying whatever is left of the budget."""
    left = budget_left(deadline, stage)
    if left is None or not _CONVERT_TIMEOUT_SUPPORTED:
        return {}
    return {"timeout": max(1, int(left))}

# ---------------------------------------------------------------------------
# LibreOffice conversion limits -- every knob below is read from .env
# ---------------------------------------------------------------------------
# How long one document may take to generate before we give up on it. The
# effective budget is size-scaled:
#
#   timeout = clamp(LIBREOFFICE_TIMEOUT_SECONDS + size_mb * PER_MB,
#                   MIN_SECONDS, MAX_SECONDS)
#
# Raise LIBREOFFICE_TIMEOUT_MAX_SECONDS on a fast dedicated box; lower it when
# a stuck document must not hold a worker for long.
LIBREOFFICE_TIMEOUT = env_int("LIBREOFFICE_TIMEOUT_SECONDS", 180)               # base budget, 3 min
LIBREOFFICE_TIMEOUT_PER_MB = env_int("LIBREOFFICE_TIMEOUT_PER_MB_SECONDS", 15)  # extra seconds per MB
LIBREOFFICE_TIMEOUT_MIN = env_int("LIBREOFFICE_TIMEOUT_MIN_SECONDS", 120)       # never below this
LIBREOFFICE_TIMEOUT_MAX = env_int("LIBREOFFICE_TIMEOUT_MAX_SECONDS", 1800)      # hard cap, 0 = no cap
# How often the monitor loop prints progress, and how long we wait for a killed
# process to actually die.
LIBREOFFICE_PROGRESS_INTERVAL = max(1, env_int("LIBREOFFICE_PROGRESS_INTERVAL_SECONDS", 5))
LIBREOFFICE_KILL_WAIT = max(1, env_int("LIBREOFFICE_KILL_WAIT_SECONDS", 5))
LIBREOFFICE_MEMORY_LIMIT_MB = env_int("LIBREOFFICE_MEMORY_LIMIT_MB", 3072)      # 3GB default for Ubuntu reliability


def compute_conversion_timeout(file_size_mb: float) -> int:
    """
    Seconds a single LibreOffice conversion is allowed to run.

    Bigger files legitimately need longer, so the base budget grows with size
    and is then clamped into [MIN, MAX]. The cap matters: without it a 200MB
    upload asked for ~3200s and pinned a worker for nearly an hour.
    """
    timeout = LIBREOFFICE_TIMEOUT + int(file_size_mb * LIBREOFFICE_TIMEOUT_PER_MB)
    timeout = max(timeout, LIBREOFFICE_TIMEOUT_MIN)
    if LIBREOFFICE_TIMEOUT_MAX > 0:
        timeout = min(timeout, LIBREOFFICE_TIMEOUT_MAX)
    return timeout


# ---------------------------------------------------------------------------
# Per-document budget
# ---------------------------------------------------------------------------
# A document passes through several stages (metadata, download, convert,
# optional repair + retry, rasterize, upload) and each has its own limit, so
# without an overall ceiling one bad document could hold a worker far longer
# than any single timeout suggests. DOC_TOTAL_TIMEOUT_SECONDS caps the whole
# pipeline: every stage below is given the smaller of its own limit and what is
# left of this budget. 0 = no overall ceiling (old behaviour).
DOC_TOTAL_TIMEOUT = env_int("DOC_TOTAL_TIMEOUT_SECONDS", 2400)  # 40 min
# A stage is not even started with less than this many seconds left -- better to
# fail fast and record the doc than to launch LibreOffice with 2 seconds.
DOC_BUDGET_MIN_STAGE = max(1, env_int("DOC_BUDGET_MIN_STAGE_SECONDS", 10))


class DocumentDeadlineExceeded(Exception):
    """One document used up its whole DOC_TOTAL_TIMEOUT_SECONDS budget."""


def make_deadline():
    """Start the clock for one document. None means no overall ceiling."""
    if DOC_TOTAL_TIMEOUT <= 0:
        return None
    return time.monotonic() + DOC_TOTAL_TIMEOUT


def budget_left(deadline, stage: str, minimum: int = None) -> float | None:
    """
    Seconds left in this document's total budget, or None when unlimited.

    Raises DocumentDeadlineExceeded when there is not enough left to start
    `stage`. Uses time.monotonic() so a clock adjustment mid-conversion cannot
    make the budget jump.
    """
    if deadline is None:
        return None
    if minimum is None:
        minimum = DOC_BUDGET_MIN_STAGE
    left = deadline - time.monotonic()
    if left < minimum:
        raise DocumentDeadlineExceeded(
            f"document budget of {DOC_TOTAL_TIMEOUT}s exhausted before {stage} "
            f"({max(0.0, left):.0f}s left, need {minimum}s)"
        )
    return left


def stage_timeout(deadline, stage: str, own_limit: int) -> int:
    """A stage's effective timeout: its own limit, capped by the budget left."""
    left = budget_left(deadline, stage)
    if left is None:
        return own_limit
    return max(1, int(min(own_limit, left)))

# ---------------------------------------------------------------------------
# HTTP retries
# ---------------------------------------------------------------------------
# urllib3 retries happen INSIDE a single session.get/patch call, so the
# per-document budget cannot interrupt them: one unhealthy endpoint can hold a
# worker for (HTTP_RETRY_TOTAL + 1) * REQUEST_TIMEOUT_SECONDS plus backoff.
# Keep HTTP_RETRY_TOTAL low when DOC_TOTAL_TIMEOUT_SECONDS is tight --
# http_worst_case_seconds() below reports the real number and warns on startup.
HTTP_RETRY_TOTAL = max(0, env_int("HTTP_RETRY_TOTAL", 5))
HTTP_RETRY_BACKOFF_FACTOR = env_float("HTTP_RETRY_BACKOFF_FACTOR", 0.5)
HTTP_RETRY_BACKOFF_MAX = env_float("HTTP_RETRY_BACKOFF_MAX_SECONDS", 30.0)
# A 429 may carry `Retry-After: 600`, which urllib3 honours literally and which
# no timeout of ours can cut short. Set to 0 to ignore the header and use our
# own backoff instead.
HTTP_RESPECT_RETRY_AFTER = env_bool("HTTP_RESPECT_RETRY_AFTER", True)


def http_backoff_seconds() -> float:
    """
    Total time urllib3 spends sleeping between retries, worst case.

    Mirrors urllib3's get_backoff_time(): no sleep before the first retry, then
    backoff_factor * 2**(n-1) for the nth, each capped at backoff_max.
    """
    total = 0.0
    for attempt in range(2, HTTP_RETRY_TOTAL + 1):
        total += min(HTTP_RETRY_BACKOFF_FACTOR * (2 ** (attempt - 1)), HTTP_RETRY_BACKOFF_MAX)
    return total


def http_worst_case_seconds() -> float:
    """
    Longest one HTTP call can take: every attempt timing out, plus backoff.

    This is the part of a document's runtime the budget cannot bound, so it is
    worth knowing before it surprises you in production.
    """
    return (HTTP_RETRY_TOTAL + 1) * REQUEST_TIMEOUT + http_backoff_seconds()


def log_timeout_config():
    """Print the effective timeout settings, and warn if they contradict."""
    print("=" * 80)
    print("⏱️  Timeout configuration")
    print(f"   document budget      : {DOC_TOTAL_TIMEOUT or 'unlimited'}s"
          f" (min per stage {DOC_BUDGET_MIN_STAGE}s)")
    print(f"   conversion           : base {LIBREOFFICE_TIMEOUT}s"
          f" + {LIBREOFFICE_TIMEOUT_PER_MB}s/MB,"
          f" clamped to [{LIBREOFFICE_TIMEOUT_MIN}s,"
          f" {LIBREOFFICE_TIMEOUT_MAX or 'unlimited'}s]")
    print(f"   http request         : {REQUEST_TIMEOUT}s,"
          f" {HTTP_RETRY_TOTAL} retries,"
          f" backoff x{HTTP_RETRY_BACKOFF_FACTOR} (max {HTTP_RETRY_BACKOFF_MAX}s),"
          f" respect Retry-After: {HTTP_RESPECT_RETRY_AFTER}")
    worst_http = http_worst_case_seconds()
    print(f"   worst case per http call : {worst_http:.0f}s"
          f" ({HTTP_RETRY_TOTAL + 1} attempts + {http_backoff_seconds():.1f}s backoff)")
    if DOC_TOTAL_TIMEOUT > 0:
        # The final upload is exempt from the budget AND is itself retried, so
        # it contributes its full worst case, not just one REQUEST_TIMEOUT.
        print(f"   worst case per document  : {DOC_TOTAL_TIMEOUT + worst_http:.0f}s"
              f" (budget + the exempt final upload, retries included)")
        if worst_http > DOC_TOTAL_TIMEOUT:
            print(f"   ⚠️  A single retrying HTTP call ({worst_http:.0f}s) can outlast the"
                  f" {DOC_TOTAL_TIMEOUT}s document budget.")
            print(f"   ⚠️  Retries run inside one request, where the budget cannot stop"
                  f" them. Lower HTTP_RETRY_TOTAL or REQUEST_TIMEOUT_SECONDS.")
        if HTTP_RESPECT_RETRY_AFTER:
            print(f"   ℹ️  A 429 with a long Retry-After header can still exceed all of"
                  f" the above; set HTTP_RESPECT_RETRY_AFTER=0 to cap it.")
    print("=" * 80)
session = requests.Session()
session.headers.update(headers)
# backoff_max only became a Retry kwarg in urllib3 2.x; older versions use the
# Retry.BACKOFF_MAX class attribute instead.
_retry_kwargs = dict(
    total=HTTP_RETRY_TOTAL,
    backoff_factor=HTTP_RETRY_BACKOFF_FACTOR,
    status_forcelist=(429, 500, 502, 503, 504),
    # POST is retryable here because the upload endpoint is idempotent: a file
    # that already has images comes back with already_done=true instead of
    # collecting duplicates.
    allowed_methods=("GET", "PATCH", "POST"),
    respect_retry_after_header=HTTP_RESPECT_RETRY_AFTER,
)
if "backoff_max" in inspect.signature(Retry.__init__).parameters:
    _retry_kwargs["backoff_max"] = HTTP_RETRY_BACKOFF_MAX
else:  # urllib3 1.x
    Retry.BACKOFF_MAX = HTTP_RETRY_BACKOFF_MAX
retry_strategy = Retry(**_retry_kwargs)
adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=16, pool_maxsize=32)
session.mount("https://", adapter)
session.mount("http://", adapter)

# Document downloads go to S3 / the CDN, not to soff.uz. Reusing `session` for
# them sent our credentials to that third-party host on every file (the old
# code did this with the superuser Bearer token). Same retry policy, no auth
# header, and the presigned/public URL needs none.
download_session = requests.Session()
download_session.mount("https://", adapter)
download_session.mount("http://", adapter)


def get_pdf_page_count(pdf_path: str) -> int | None:
    """Return the total page count for a PDF, if available."""
    try:
        info = pdfinfo_from_path(pdf_path)
        return int(info.get("Pages", 0))
    except Exception as exc:
        print(f"⚠️ Unable to read PDF info for {pdf_path}: {exc}")
        return None


def make_process_limiter(cpu_seconds: int):
    """
    Build a preexec_fn that limits the LibreOffice child process.

    RLIMIT_AS caps *virtual* address space, not resident memory, and LibreOffice
    maps far more address space than it actually uses. Capping it at the nominal
    limit kills healthy conversions, so use 3x -- the same headroom the old
    xvfb `ulimit -v` path used.

    RLIMIT_CPU must track the caller's size-scaled timeout. Previously it was
    pinned to the static LIBREOFFICE_TIMEOUT, so the kernel SIGKILLed large
    files at 180 CPU-seconds no matter what the dynamic timeout said.
    """
    def set_process_limits():
        try:
            # 3x nominal: virtual address space, not resident memory
            vmem_bytes = LIBREOFFICE_MEMORY_LIMIT_MB * 1024 * 1024 * 3
            resource.setrlimit(resource.RLIMIT_AS, (vmem_bytes, vmem_bytes))
            resource.setrlimit(resource.RLIMIT_CPU, (cpu_seconds, cpu_seconds))
        except (ValueError, OSError, AttributeError) as e:
            # On some systems setting limits fails - log but don't fail the conversion
            print(f"⚠️ Could not set all resource limits: {e}")
    return set_process_limits


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
    deadline=None,
):
    file_name = os.path.basename(ppt_path)
    file_size_mb = os.path.getsize(ppt_path) / (1024 * 1024)
    
    # Size-scaled budget, all knobs from .env (see compute_conversion_timeout)
    dynamic_timeout = compute_conversion_timeout(file_size_mb)
    budget = budget_left(deadline, f"conversion of {file_name}")
    trimmed = budget is not None and budget < dynamic_timeout

    print(f"\n{'='*80}")
    print(f"🔄 Starting conversion: {file_name}")
    print(f"📊 File size: {file_size_mb:.2f}MB | Memory limit: {LIBREOFFICE_MEMORY_LIMIT_MB}MB")
    print(f"⏱️  Timeout: {dynamic_timeout}s (base {LIBREOFFICE_TIMEOUT}s + {LIBREOFFICE_TIMEOUT_PER_MB}s/MB, "
          f"min {LIBREOFFICE_TIMEOUT_MIN}s, max {str(LIBREOFFICE_TIMEOUT_MAX) + 's' if LIBREOFFICE_TIMEOUT_MAX else 'unlimited'})")
    if trimmed:
        dynamic_timeout = max(1, int(budget))
        print(f"⏱️  Trimmed to {dynamic_timeout}s — only that much of the "
              f"{DOC_TOTAL_TIMEOUT}s document budget is left")
    print(f"{'='*80}")
    
    abs_ppt = os.path.abspath(ppt_path)
    abs_output = tempfile.mkdtemp(prefix="libreoffice_out_", dir=LOCAL_TMP_DIR)
    os.makedirs(output_folder, exist_ok=True)
    print(f"📁 Created output directory: {abs_output}")
    print(f"📁 Created image folder: {output_folder}")

    profile_dir = tempfile.mkdtemp(prefix="libreoffice_profile_", dir=LOCAL_TMP_DIR)
    print(f"📁 Created LibreOffice profile: {profile_dir}")

    # Configure LibreOffice profile to disable Java
    print(f"⚙️  Configuring LibreOffice profile...")
    configure_libreoffice_profile(profile_dir)

    # Create environment for headless LibreOffice operation
    env = os.environ.copy()
    # Do NOT set SAL_USE_VCLPLUGIN: 'gen' is the *X11* plugin and forces a display
    # dependency. With it unset, --headless selects the headless 'svp' backend and
    # needs no display server at all (verified on LibreOffice 25.8).
    env.pop('DISPLAY', None)
    env.pop('SAL_USE_VCLPLUGIN', None)
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

    # No xvfb wrapper: headless LibreOffice needs no display server, and wrapping
    # in `bash -c ... xvfb-run` meant a SIGKILL on timeout orphaned the Xvfb process
    # and its /tmp lock every single time a conversion timed out.

    # Monitor running LibreOffice processes before starting
    running_count = count_running_libreoffice_processes()
    if running_count > 0:
        print(f"📊 Currently running LibreOffice processes: {running_count}")
    
    print(f"🚀 Launching LibreOffice process...")
    
    stdout_capture = tempfile.TemporaryFile(mode="w+", dir=LOCAL_TMP_DIR)
    stderr_capture = tempfile.TemporaryFile(mode="w+", dir=LOCAL_TMP_DIR)

    process_start_time = time.time()
    process = None
    process_pid = None
    
    try:
        # Use Popen to monitor process in real-time
        # Output goes to temp files, NOT subprocess.PIPE: nothing drains a pipe
        # while we poll, so a chatty conversion filled the 64KB buffer, blocked
        # on write and then got killed at the timeout even though it was healthy.
        process = subprocess.Popen(
            soffice_cmd,
            stdout=stdout_capture,
            stderr=stderr_capture,
            text=True,
            preexec_fn=make_process_limiter(dynamic_timeout),
            env=env
        )
        process_pid = process.pid
        print(f"✅ LibreOffice process started | PID: {process_pid}")
        
        # Monitor process with periodic updates
        check_interval = LIBREOFFICE_PROGRESS_INTERVAL  # from .env
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
                    process.wait(timeout=LIBREOFFICE_KILL_WAIT)
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
        
        # Read back what the child wrote to the capture files
        process.wait()
        stdout_capture.seek(0)
        stderr_capture.seek(0)
        stdout = stdout_capture.read()
        stderr = stderr_capture.read()
        
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
        for capture in (stdout_capture, stderr_capture):
            try:
                capture.close()
            except Exception:
                pass

    try:
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
            **raster_kwargs(deadline, "slide rasterization"),
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

    finally:
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
    deadline=None,
):
    os.makedirs(output_folder, exist_ok=True)
    total_pages = get_pdf_page_count(pdf_path)
    images = convert_from_path(
        pdf_path,
        first_page=1,
        last_page=max_pages,
        dpi=PDF_DPI,
        **raster_kwargs(deadline, "pdf rasterization"),
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


def download_file(file_url, save_path, deadline=None):
    timeout = stage_timeout(deadline, "download", REQUEST_TIMEOUT)
    with download_session.get(file_url, stream=True, timeout=timeout) as response:
        response.raise_for_status()
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                if chunk:
                    f.write(chunk)
                    # REQUEST_TIMEOUT is per-read, so a slow-but-steady trickle
                    # could outlast the whole document budget without it ever
                    # timing out. Check the deadline as the bytes arrive.
                    if deadline is not None and time.monotonic() > deadline:
                        raise DocumentDeadlineExceeded(
                            f"document budget of {DOC_TOTAL_TIMEOUT}s exhausted "
                            f"mid-download ({os.path.getsize(save_path) / (1024*1024):.1f}MB fetched)"
                        )


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


def try_repair_office_file(path: str, deadline=None) -> str | None:
    """
    Repair corrupted Office file (pptx/docx).
    - For pptx/docx: replace broken images with placeholders and clean XML references.
    - For ppt: try auto-convert to pptx via LibreOffice, then repair.
    Returns repaired file path or None if repair failed.
    """
    # Handle old .ppt files by converting to .pptx first
    if path.lower().endswith(".ppt") and not path.lower().endswith(".pptx"):
        pptx_path = path.replace(".ppt", ".pptx")
        # Same size-scaled budget as the main conversion: an old .ppt big enough
        # to need repairing is exactly the case the static 180s was too short for.
        try:
            repair_timeout = compute_conversion_timeout(os.path.getsize(path) / (1024 * 1024))
        except OSError:
            repair_timeout = LIBREOFFICE_TIMEOUT
        repair_budget = budget_left(deadline, "ppt→pptx repair")
        if repair_budget is not None:
            repair_timeout = max(1, int(min(repair_timeout, repair_budget)))
        print(f"⏱️  PPT→PPTX repair timeout: {repair_timeout}s")
        try:
            # Create temporary profile for this conversion using local temp dir
            temp_profile = tempfile.mkdtemp(prefix="libreoffice_profile_", dir=LOCAL_TMP_DIR)
            configure_libreoffice_profile(temp_profile)

            # Create environment for headless LibreOffice operation
            env = os.environ.copy()
            env.pop('DISPLAY', None)
            env.pop('SAL_USE_VCLPLUGIN', None)
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

            process = None
            try:
                process = subprocess.run(
                    soffice_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=repair_timeout,
                    preexec_fn=make_process_limiter(repair_timeout),
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
        temp_dir = tempfile.mkdtemp(prefix="repair_office_", dir=LOCAL_TMP_DIR)

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
    

def record_failure(doc_id, reason):
    """Append a failed doc_id so it can be retried instead of being lost."""
    try:
        with open(FAILED_LOG, "a") as fh:
            fh.write(f"{doc_id}\t{reason}\n")
    except Exception as exc:
        print(f"⚠️ Could not write to {FAILED_LOG}: {exc}")


def generate_docs_for_soff(job):
    """
    Render one document's pages and upload them.

    `job` is a dict from the queue endpoint (document_id, file_id, file_url,
    file_type). A bare int is still accepted -- the id is then resolved with one
    request, which is how the failed-id replay path works.
    """
    temp_path = None
    output_folder = None
    success = False
    # Whole-document clock; every stage below is capped by what is left of it.
    deadline = make_deadline()
    doc_start = time.monotonic()
    if deadline is not None:
        print(f"⏱️  Document budget: {DOC_TOTAL_TIMEOUT}s total")
    doc_id = job["document_id"] if isinstance(job, dict) else job
    try:
        print(f"\n{'#'*80}")
        print(f"🆔 Processing document ID: {doc_id}")
        print(f"{'#'*80}")

        if not isinstance(job, dict):
            # Only the replay path gets here; a batch from the queue already
            # carries everything below.
            job = fetch_job(doc_id, deadline=deadline)
            if job is None:
                return True

        file_id = job.get("file_id")
        file_type = (job.get("file_type") or "").lower()
        file_url = job.get("file_url")

        if not file_url:
            print(f"⚠️  No file URL found for doc_id={doc_id}, skipping")
            print(f"🔍 Job: {job}")
            return True
        temp_path = f"temp_copy_{doc_id}{file_type}"
        output_folder = f"images_slide_copy_{doc_id}"

        print(f"📥 Downloading file from: {file_url}")
        print(f"💾 Saving to: {temp_path}")
        download_start = time.time()
        download_file(file_url, temp_path, deadline=deadline)
        download_time = time.time() - download_start
        file_size = os.path.getsize(temp_path) / (1024 * 1024)
        print(f"✅ Download completed in {download_time:.1f}s | File size: {file_size:.2f}MB")
        repaired = None
        if file_type in ['.pptx', '.ppt', '.doc', '.docx'] and os.path.exists(temp_path):
            try:
                image_paths, pages_count = not_pdf_to_images_webp_libreoffice(
                    temp_path,
                    output_folder,
                    quality=85,
                    max_width=1600,
                    deadline=deadline,
                )

            except DocumentDeadlineExceeded:
                # Out of time is not corruption -- repairing and retrying would
                # only blow the budget further. Let the outer handler record it.
                raise
            except Exception as e:
                print(f"\n❌ LibreOffice failed on {temp_path}: {e}")
                print(f"🔧 Attempting to repair file...")
                repaired = try_repair_office_file(temp_path, deadline=deadline)
                if repaired:
                    print(f"✅ File repaired: {repaired}")
                    print(f"🔄 Retrying conversion with repaired file...")
                    try:
                        image_paths, pages_count = not_pdf_to_images_webp_libreoffice(
                            repaired,
                            output_folder,
                            quality=85,
                            max_width=1600,
                            deadline=deadline,
                        )
                        print(f"✅ Successfully processed repaired file: {len(image_paths)} image(s)")
                    except DocumentDeadlineExceeded:
                        raise
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
                        record_failure(doc_id, f"repair+convert failed: {e2!r}")
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
                    record_failure(doc_id, f"unrepairable: {e!r}")
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
                quality=85, max_width=1600,
                deadline=deadline,
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

            print(f"   📊 Page count: {pages_count}")

            # The images already exist by now, so refusing to upload would
            # throw away the whole conversion. The upload always gets its own
            # full timeout; it can overshoot the document budget by at most
            # REQUEST_TIMEOUT_SECONDS.
            if USE_SERVICE_API:
                data = {"document_id": doc_id, "page_count": pages_count}
                if file_id:
                    # Studio products hold several files; say which one these
                    # pages belong to instead of letting the server guess.
                    data["file_id"] = file_id
                upload_response = session.post(
                    UPLOAD_URL, files=files, data=data, timeout=REQUEST_TIMEOUT
                )
            else:
                upload_response = session.patch(
                    LEGACY_DETAIL_URL.format(doc_id=doc_id),
                    files=files,
                    data={"page_count": pages_count},
                    timeout=REQUEST_TIMEOUT,
                )
            # Without this an upload that 500s still printed "COMPLETED"
            upload_response.raise_for_status()
            if USE_SERVICE_API:
                try:
                    if upload_response.json().get("already_done"):
                        print("   ℹ️  Server already had images for this file")
                except ValueError:
                    pass
        upload_time = time.time() - upload_start
        print(f"✅ Upload completed in {upload_time:.1f}s")
        
        # Clean up image folder
        if os.path.exists(output_folder):
            print(f"🧹 Cleaning up image folder: {output_folder}")
            shutil.rmtree(output_folder, ignore_errors=True)
            print(f"🗑️  Deleted: {output_folder}")
        
        success = True
        print(f"\n{'='*80}")
        print(f"✅ COMPLETED doc_id={doc_id}")
        print(f"⏱️  Total time: {time.monotonic() - doc_start:.1f}s")
        print(f"{'='*80}\n")

    except DocumentDeadlineExceeded as e:
        elapsed = time.monotonic() - doc_start
        print("\n" + "=" * 80)
        print(f"⏱️  BUDGET EXCEEDED for doc_id={doc_id} after {elapsed:.0f}s: {e}")
        print("=" * 80)
        record_failure(doc_id, f"doc budget exceeded after {elapsed:.0f}s: {e}")

    except Exception as e:
        print(f"\n{'='*80}")
        print(f"❌ ERROR processing doc_id={doc_id}: {e}")
        print(f"{'='*80}")
        import traceback
        print(f"Traceback:\n{traceback.format_exc()}")
        record_failure(doc_id, repr(e))

    finally:
        # Cleanup temp files
        print(f"\n🧹 Final cleanup for doc_id={doc_id}...")
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)
            print(f"🗑️  Deleted temp file: {temp_path}")
        if output_folder and os.path.exists(output_folder):
            shutil.rmtree(output_folder, ignore_errors=True)
            print(f"🗑️  Deleted output folder: {output_folder}")

    return success


# ========= Job fetching =========


def fetch_job(doc_id, deadline=None):
    """
    Resolve one document id to a job dict. Only used for replaying an id by
    hand (e.g. from failed_doc_ids.log); a batch from the queue already carries
    everything. Returns None when there is nothing to render.
    """
    timeout = stage_timeout(deadline, "metadata fetch", REQUEST_TIMEOUT)

    if USE_SERVICE_API:
        response = session.get(JOB_URL.format(doc_id=doc_id), timeout=timeout)
        if response.status_code == 404:
            print(f"⚠️  doc_id={doc_id} is not waiting for a poster, skipping")
            return None
        response.raise_for_status()
        return response.json()

    response = session.get(LEGACY_DETAIL_URL.format(doc_id=doc_id), timeout=timeout)
    response.raise_for_status()
    data = response.json()
    document_data = data.get("document")
    if not document_data:
        print(f"❌ 'document' key missing in admin API response for doc_id={doc_id}")
        print(f"🔍 Full Response: {data}")
        return None
    return {
        "document_id": doc_id,
        "file_id": document_data.get("id"),
        "file_type": (document_data.get("file_type") or "").lower(),
        "file_url": document_data.get("file_url"),
    }


def fetch_queue_batch(limit, order, exclude_ids=(), offset=0):
    """One request -> up to `limit` jobs, each already carrying its file_url."""
    params = {"limit": limit}
    if order:
        params["order"] = order
    if offset:
        params["offset"] = offset
    if exclude_ids:
        params["exclude_ids"] = ",".join(str(i) for i in exclude_ids)

    response = session.get(QUEUE_URL, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json().get("results", [])


# ========= Worker & Queue System =========


def worker(job_queue: Queue, result_queue: Queue):
    while True:
        job = job_queue.get()
        if job is None:  # Poison pill -> stop worker
            break
        doc_id = job["document_id"] if isinstance(job, dict) else job
        ok = generate_docs_for_soff(job)
        if not ok:
            print(f"⚠️  doc_id={doc_id} FAILED -- recorded in {FAILED_LOG}")
        # The producer needs both outcomes: a success leaves the server-side
        # queue on its own, a failure does not and must be excluded by hand.
        result_queue.put((doc_id, bool(ok)))


def _drain_results(result_queue, in_flight, failed):
    while True:
        try:
            doc_id, ok = result_queue.get_nowait()
        except Empty:
            return
        in_flight.discard(doc_id)
        if not ok:
            failed.add(doc_id)


def dispatch_from_service(job_queue, result_queue, limit):
    """
    Pull batches from the service queue and hand them to the workers.

    No cursor is needed: the queue is self-draining, because a document that
    gets its images uploaded drops out of the server-side filter. What does
    have to be carried is everything the server would otherwise hand out
    twice -- the documents a worker is busy with right now (`in_flight`) and
    the ones that failed for good this run (`failed`, which never leave the
    queue because they never get a poster). Both go back as `exclude_ids`;
    once that outgrows the server's cap the overflow is stepped over with
    `offset` instead.
    """
    dispatched = 0
    in_flight = set()
    failed = set()

    while dispatched < limit:
        _drain_results(result_queue, in_flight, failed)

        # in_flight first: excluding a busy document matters more than
        # excluding one that already failed, since a duplicate means two
        # workers rendering the same file at the same time.
        exclude = list(in_flight) + sorted(failed)
        offset = 0
        if len(exclude) > QUEUE_MAX_EXCLUDE_IDS:
            offset = len(exclude) - QUEUE_MAX_EXCLUDE_IDS
            exclude = exclude[:QUEUE_MAX_EXCLUDE_IDS]

        batch_size = min(QUEUE_BATCH_SIZE, limit - dispatched)
        try:
            jobs = fetch_queue_batch(batch_size, QUEUE_ORDER, exclude, offset)
        except requests.RequestException as exc:
            print(f"❌ Queue request failed: {exc}")
            break

        if not jobs:
            if in_flight:
                # Everything left is being worked on. Wait for one result and
                # ask again instead of declaring the queue empty.
                print(f"⏳ Queue empty, {len(in_flight)} document(s) still in flight...")
                try:
                    doc_id, ok = result_queue.get(timeout=REQUEST_TIMEOUT)
                except Empty:
                    break
                in_flight.discard(doc_id)
                if not ok:
                    failed.add(doc_id)
                continue
            print("✅ Queue is empty, nothing left to generate")
            break

        print(f"📥 Fetched {len(jobs)} job(s) | order={QUEUE_ORDER or 'server default'}"
              f" | excluded={len(exclude)} | offset={offset}")
        for job in jobs:
            if dispatched >= limit:
                break
            in_flight.add(job["document_id"])
            print(f"📥 Queued doc_id={job['document_id']}")
            job_queue.put(job)  # blocks on back-pressure, which paces the run
            dispatched += 1
            _drain_results(result_queue, in_flight, failed)

    print(f"\n📊 Dispatched {dispatched} document(s) | {len(failed)} failed this run")
    if failed:
        print(f"   Failed ids: {sorted(failed)}")


def dispatch_from_legacy_api(job_queue, limit, start_id):
    """The old admin-API walk: one request for the next id, per document."""
    start = start_id

    for _ in range(limit):
        endpoint = f"{LEGACY_NEXT_URL}?type=true"
        if start:
            endpoint += f"&pk={start}"

        response = session.get(endpoint, timeout=REQUEST_TIMEOUT)
        if response.status_code == 200:
            data = response.json()
            doc_id = data.get('id')
            if not doc_id:
                break
            print(f"📥 Queued doc_id={doc_id}")
            job_queue.put(doc_id)
            start = doc_id

        time.sleep(0.2)  # avoid hammering API


def process_doc_poster_generate_queue(limit=None, workers=None, start_id=None):
    limit = limit or LIMIT
    workers = workers or max(2, cpu_count() // 2)
    job_queue = Queue(maxsize=workers * 2)
    result_queue = Queue()

    # Start workers
    processes = [
        Process(target=worker, args=(job_queue, result_queue)) for _ in range(workers)
    ]
    for p in processes:
        p.start()

    try:
        if USE_SERVICE_API:
            dispatch_from_service(job_queue, result_queue, limit)
        else:
            dispatch_from_legacy_api(job_queue, limit, start_id or START_ID)
    finally:
        # Stop workers
        for _ in processes:
            job_queue.put(None)
        for p in processes:
            p.join()


def replay_failed_ids(doc_ids, workers=None):
    """Re-run specific ids, e.g. the ones recorded in failed_doc_ids.log."""
    workers = workers or max(2, cpu_count() // 2)
    job_queue = Queue(maxsize=workers * 2)
    result_queue = Queue()
    processes = [
        Process(target=worker, args=(job_queue, result_queue)) for _ in range(workers)
    ]
    for p in processes:
        p.start()
    try:
        for doc_id in doc_ids:
            job_queue.put(int(doc_id))
    finally:
        for _ in processes:
            job_queue.put(None)
        for p in processes:
            p.join()


if __name__ == "__main__":
    log_timeout_config()
    workers_env = env_int("WORKER_COUNT", 4)
    if len(sys.argv) > 1:
        # `python new_docx2pdf.py 123 456` re-runs those ids and nothing else.
        replay_failed_ids(sys.argv[1:], workers=workers_env)
    else:
        process_doc_poster_generate_queue(workers=workers_env)
