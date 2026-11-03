# Research Library — INFO-H420 Assignment 3

An Airflow pipeline that reads shared Google Drive PDFs, converts them to Markdown, and generates metadata using a local LLM.

## Requirements

Python packages: `apache-airflow`, `pymupdf4llm`, `openai`, and `requests`.

External tools: **rclone** and **Ollama**, with the `qwen2.5:3b` model available locally.

## Before starting

The project owner provides:

- GitHub repository URL
- Shared `ResearchLibrary` Drive folder link
- OAuth Client ID and client secret, sent privately

Your Google account must have access to the shared folder and be registered as an OAuth test user.

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

Sign in with your own Google account. Each member keeps their own authentication tokens locally.

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

The result should contain `papers` and `output`.

## Run Airflow

Ensure Ollama is running. In another terminal, activate your Python environment containing the required packages, then run:

```bash
cd ~/Documents/research-library

export AIRFLOW_HOME="$HOME/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"

airflow standalone
```

Open [localhost:8080](http://localhost:8080), sign in using Airflow’s generated credentials, and trigger the `research_library` DAG.

Inspect task logs for results. Markdown files are saved in `data/markdown/`; metadata currently appears in the `generate_metadata` logs.

## Current prototype

The pipeline lists all PDFs, converts the first one, and generates metadata from its first 4,000 characters.

Full-document handling, JSON validation/retry, SQLite storage, HTML generation, Discord notifications, and scheduling remain to be implemented.

Keep OAuth credentials, authentication tokens, and local generated data outside Git.