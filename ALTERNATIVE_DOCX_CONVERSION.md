# Alternative DOCX to PDF Conversion Methods

The code now supports multiple fallback methods for converting DOCX to PDF when LibreOffice hangs or fails.

## Available Methods (in order of attempt):

### 1. **unoconv** (Recommended)
- Another LibreOffice wrapper, often more stable than direct LibreOffice calls
- Installation:
  ```bash
  # macOS
  brew install unoconv
  
  # Linux (Ubuntu/Debian)
  sudo apt-get install unoconv
  
  # Or via pip
  pip install unoconv
  ```

### 2. **docx2pdf**
- Python library that uses different backends:
  - Windows: Microsoft Word COM API
  - macOS: Microsoft Word via AppleScript
  - Linux: LibreOffice
- Installation:
  ```bash
  pip install docx2pdf
  ```

### 3. **pypandoc**
- Requires pandoc and LaTeX to be installed
- Installation:
  ```bash
  # Install pandoc first
  # macOS
  brew install pandoc
  
  # Linux
  sudo apt-get install pandoc
  
  # Then install Python library
  pip install pypandoc
  ```

### 4. **LibreOffice** (Fallback)
- The original method, used as last resort

## How It Works

When processing a DOCX file:
1. The system tries `unoconv` first
2. If that fails, tries `docx2pdf`
3. If that fails, tries `pypandoc`
4. Finally falls back to direct LibreOffice call

## Recommendations

For your use case (40+ pages, many diagrams, many images):

1. **Try unoconv first** - It's often more stable with complex documents:
   ```bash
   brew install unoconv  # macOS
   ```

2. **Or use docx2pdf** - If you're on macOS/Windows, it can use Microsoft Word which handles complex documents better:
   ```bash
   pip install docx2pdf
   ```

3. **Disable compression for problematic files** - If compression is causing issues, you can disable it:
   ```bash
   export ENABLE_DOCX_COMPRESSION=false
   ```

## Testing

After installing an alternative, the code will automatically try it first before falling back to LibreOffice.

