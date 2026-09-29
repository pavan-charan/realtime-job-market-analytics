# 22. Troubleshooting & Diagnostic Guide

### Problem 1: `ModuleNotFoundError: No module named 'distutils'`
- **Cause:** Python 3.12 removed the legacy `distutils` package, used internally by PySpark ML image helpers.
- **Solution:** Run `pip install setuptools` inside the virtual environment.

### Problem 2: `NativeIO$Windows.access0(String, int)` Error on Windows
- **Cause:** Hadoop Windows native binaries fail Windows file permission checks when creating directories.
- **Solution:** PySpark startup dynamically sets JVM reflection flag `org.apache.hadoop.io.nativeio.NativeIO$Windows.skipCheck = true`.

### Problem 3: `StringDataRightTruncation: value too long for character varying(255)`
- **Cause:** Large unstructured job postings exceeded standard `VARCHAR(255)` columns during database sync.
- **Solution:** PostgreSQL schema updated to unbounded `TEXT` data types with `execute_values` chunked batch loading.
