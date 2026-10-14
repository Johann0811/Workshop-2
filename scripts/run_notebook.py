"""
Script to execute notebooks/data_profiling_exec.ipynb in Docker
without touching the original notebooks/data_profiling.ipynb.
"""

import io
import json
import os
import sys
from contextlib import redirect_stdout, redirect_stderr
from pathlib import Path

# Set environment variables for the profiling notebook
os.environ["GRAMMY_MYSQL_URL"] = os.getenv(
    "GRAMMY_MYSQL_URL",
    "mysql+pymysql://grammy_user:grammy_pass@mysql:3306/grammy_source"
)
os.environ["DATA_DIR"] = os.getenv("DATA_DIR", "/opt/airflow/data/raw")
os.environ["SAVE_EVIDENCE"] = "1"

notebook_path = Path("/opt/airflow/notebooks/data_profiling_exec.ipynb")
if not notebook_path.is_file():
    print(f"Error: {notebook_path} does not exist.")
    sys.exit(1)

with open(notebook_path, "r", encoding="utf-8") as f:
    nb = json.load(f)

nb_globals = {
    "__name__": "__main__",
    "__file__": str(notebook_path),
}

# Helper for matplotlib figures
figures_saved = []
def mock_render_figure(name):
    print(f"[NOTEBOOK FIGURE] Saved figure {name}")
    figures_saved.append(name)

nb_globals["render_figure"] = mock_render_figure

exec_count = 1
total_code_cells = sum(1 for c in nb["cells"] if c["cell_type"] == "code")
print(f"Executing {total_code_cells} code cells in {notebook_path.name}...")

for idx, cell in enumerate(nb["cells"]):
    if cell["cell_type"] != "code":
        continue

    source = "".join(cell["source"])
    if not source.strip():
        continue

    stdout_buf = io.StringIO()
    stderr_buf = io.StringIO()

    try:
        with redirect_stdout(stdout_buf), redirect_stderr(stderr_buf):
            exec(source, nb_globals)
        out_text = stdout_buf.getvalue()
        err_text = stderr_buf.getvalue()

        cell["execution_count"] = exec_count
        outputs = []
        if out_text:
            outputs.append({
                "output_type": "stream",
                "name": "stdout",
                "text": out_text.splitlines(keepends=True),
            })
            print(f"Cell {idx} (Count {exec_count}):")
            print(out_text.strip()[:200])
        if err_text:
            outputs.append({
                "output_type": "stream",
                "name": "stderr",
                "text": err_text.splitlines(keepends=True),
            })
        cell["outputs"] = outputs
        exec_count += 1
    except Exception as e:
        print(f"Error executing cell {idx}: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

with open(notebook_path, "w", encoding="utf-8") as f:
    json.dump(nb, f, indent=1)

print(f"\nSuccessfully executed all {exec_count - 1} code cells!")
print(f"Preserved original notebooks/data_profiling.ipynb untouched.")
