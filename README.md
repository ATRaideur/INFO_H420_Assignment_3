# Research Library — INFO-H420 Assignment 3

An Airflow pipeline that reads PDFs from a shared Google Drive folder, converts them to Markdown, extracts metadata using a local LLM, stores it in SQLite, and generates a searchable HTML library.

## Requirements

Python packages: `apache-airflow`, `pymupdf4llm`, and `openai`. `requests` is planned for Discord notifications.

`sqlite3`, `json`, `pathlib`, and `html` are included with Python.

External tools: **rclone** and **Ollama**, with the `qwen2.5:3b` model available locally. Use the same Airflow version as the project owner.

## Before starting

The project owner provides:

- GitHub repository URL
- Shared `ResearchLibrary` Drive folder link, with Editor access
- OAuth Client ID and client secret, sent privately

Your Google account must have access to the shared folder and be registered as an OAuth test user while the app is in Testing mode.

Clone the repository into `~/Documents/research-library`.

## Connect Google Drive

Run:

```bash
rclone config
```

Configure the connection:

| Prompt | Answer |
|---|---|
| New remote | `n` |
| Name | `gdrive` |
| Storage | `drive` |
| Client ID | Provided Client ID |
| Client secret | Provided client secret |
| Scope | `1` — full Drive access |
| Root folder ID, if asked | Leave empty |
| Service account file | Leave empty |
| Advanced configuration | `n` |
| Browser authentication | `y` |
| Shared Drive / Team Drive | `n` |
| Keep configuration | `y` |

Sign in with your own Google account. Each member keeps their own authentication tokens locally; never share an authenticated `rclone.conf`.

Scope `1` grants broad access to the authenticated account's Drive. Mounting only the project folder does not narrow that authorization. A dedicated project account can keep personal files separate.

## Mount the shared folder

Open the shared `ResearchLibrary` folder in your browser. Its folder ID is the value after `/folders/` in the URL.

Replace `FOLDER_ID` below with that value:

```bash
mkdir -p ~/Documents/ResearchLibraryDrive

rclone mount gdrive: "$HOME/Documents/ResearchLibraryDrive" \
  --drive-root-folder-id "FOLDER_ID" \
  --vfs-cache-mode full
```

Keep this terminal running. In another terminal, verify:

```bash
ls ~/Documents/ResearchLibraryDrive
```

The result should contain `papers` and `output`. Add input PDFs to the shared `papers` folder. Rclone fetches and caches files locally when they are accessed.

## Run the pipeline

Ensure Ollama is running. In another terminal, activate your Python environment containing the required packages, then run:

```bash
cd ~/Documents/research-library

export AIRFLOW_HOME="$HOME/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"

airflow standalone
```

Open [localhost:8080](http://localhost:8080), sign in using Airflow's generated credentials, enable the `research_library` DAG if paused, and trigger a run.

The pipeline:

1. Lists PDFs whose filenames are not already stored in SQLite.
2. Converts the first unprocessed PDF to Markdown.
3. Extracts `title`, `summary`, `keywords`, and `category` from the first 4,000 Markdown characters.
4. Parses the JSON and validates the required fields and types, retrying once if invalid.
5. Saves the metadata in SQLite, with unique filenames.
6. Regenerates the HTML page from all saved records.

Inspect each task's logs in Airflow for progress or errors. Running `python dags/paper_pipeline.py` loads the DAG definition; it does not execute the tasks.

## View the results

| Output | Location |
|---|---|
| Markdown | `~/Documents/research-library/data/markdown/` |
| SQLite database, created automatically | `~/Documents/research-library/data/papers.db` |
| Shared HTML library | `~/Documents/ResearchLibraryDrive/output/index.html` |

Open the page locally:

```bash
xdg-open "$HOME/Documents/ResearchLibraryDrive/output/index.html"
```

Use the search field to filter titles, summaries, categories, keywords, and filenames. Refresh the page after a new run. Google Drive stores the HTML file; open it locally through the mount to use the page in your browser.

## Current prototype limits

- One new PDF is processed per run because conversion currently uses `pdf_paths[:1]`.
- Metadata describes only the first 4,000 Markdown characters, not the full document.
- Existing filenames are skipped, including files whose contents have changed under the same name.
- The DAG is triggered manually (`schedule=None`). Full-document handling, processing all new PDFs per run, Discord notifications, and scheduling remain to be implemented.
- PDFs and HTML are shared through Drive. SQLite and Markdown stay local to each member. Use one designated machine to run the pipeline and publish the shared HTML; separate local databases can produce different versions of that page.

Keep OAuth credentials, authentication tokens, `.env`, virtual environments, Airflow runtime files, and generated local data outside Git.