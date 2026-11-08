# Research Library — INFO-H420 Assignment 3

A scheduled Airflow pipeline that reads new PDFs from a shared Google Drive folder, converts them to Markdown, extracts metadata using a local LLM, stores it in SQLite, generates a searchable static HTML library, and sends Discord notifications.

All pipeline features required by the assignment are implemented.

## Requirements

Python packages: `apache-airflow`, `pymupdf4llm`, `openai`, `requests`, and `pendulum`. The Python and Airflow versions should match the project owner's tested setup; the code uses Airflow 3's `airflow.sdk` API.

`sqlite3`, `json`, `pathlib`, `html`, and `os` are included with Python. The repository must also contain `metadata_utils.py`, which provides `validate_metadata`.

External tools:

- **rclone** for access to the shared Google Drive folder.
- **Ollama**, serving `qwen2.5:3b` at `http://localhost:11434/v1`.
- A **Discord webhook** targeting the notification channel.

The Python environment containing these packages should be active before Airflow starts. Its location depends on the local setup and does not need to be inside the repository. For a fresh installation, the [official Airflow installation instructions](https://airflow.apache.org/docs/apache-airflow/stable/start.html) describe the setup; the Python and Airflow versions should match the project owner's tested configuration, with the corresponding Python constraints.

## Before starting

The project owner provides:

- GitHub repository URL.
- Shared `ResearchLibrary` Drive folder link, with Editor access.
- OAuth Client ID and client secret, sent privately.
- Discord webhook URL, sent privately, or access to configure your own webhook.

Your Google account must have access to the shared folder and be registered as an OAuth test user while the app is in Testing mode.

The repository can be cloned into any local directory. The commands below use two shell variables for the repository and the Google Drive mount point:

```bash
export PROJECT_DIR="/absolute/path/to/research-library"
export DRIVE_MOUNT_DIR="/absolute/path/to/ResearchLibraryDrive"
```

These are placeholders for each member's own absolute paths; no particular parent directory is required. The same variable definitions should be available in each terminal used for the commands below.

These variables are used only by the shell commands in this README. They do not automatically configure the DAG. Any repository, input, output, Markdown, or database paths defined in `dags/paper_pipeline.py` should match the selected local directories before Airflow starts.

## Connect Google Drive

The Google Drive connection can be configured with:

```bash
rclone config
```

The connection settings are:

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

Sign in with your own Google account. Each member keeps their authentication tokens locally; never share an authenticated `rclone.conf`.

Scope `1` grants broad access to the authenticated account's Drive. Mounting only the project folder does not narrow that authorization. A dedicated project account can keep personal files separate.

## Mount the shared folder

The shared **ResearchLibrary** folder ID is the value after `/folders/` in its browser URL. It should refer to that folder itself, rather than its parent folder.

The local mount directory can be created with:

```bash
mkdir -p "$DRIVE_MOUNT_DIR"
```

The shared folder ID can be entered at the following prompt:

```bash
read -r -p "Paste your ResearchLibrary folder ID: " drive_folder_id
```

The folder can then be mounted with:

```bash
rclone mount gdrive: "$DRIVE_MOUNT_DIR" \
  --drive-root-folder-id "$drive_folder_id" \
  --vfs-cache-mode full
```

The rclone mount should remain active in this terminal while the pipeline is running. Its contents can be checked from another terminal with:

```bash
ls "$DRIVE_MOUNT_DIR"
ls "$DRIVE_MOUNT_DIR/papers"
```

The mounted folder should contain `papers` and `output`. Input PDFs should be placed in `papers` with unique filenames and the `.pdf` extension. Rclone fetches and caches files locally when they are accessed and uploads generated HTML changes to Drive.

Mounting by folder ID also works for members who have access through a shared link without having the same parent-folder path in their own Drive.

## Configure Discord notifications

Create a webhook in your Discord server under **Server Settings → Integrations → Webhooks**, select the destination text channel, and copy its URL. Alternatively, use the webhook provided privately by the project owner.

The webhook URL can be entered in the terminal used to launch Airflow. The following prompt hides the input:

```bash
read -r -s -p "Paste Discord webhook URL, then press Enter: " DISCORD_WEBHOOK_URL
```

The variable should then be exported:

```bash
export DISCORD_WEBHOOK_URL
```

The DAG reads `DISCORD_WEBHOOK_URL` from the environment, so it should be exported before Airflow starts, including when using a new terminal. A URL change requires Airflow to restart with the new value in its launch environment. The URL should remain private and outside Git.

## Run the pipeline

Before Airflow starts:

- Ollama should be running with the `qwen2.5:3b` model available.
- The rclone mount should remain active.
- The Python environment containing the required packages should be active.
- `DISCORD_WEBHOOK_URL` should be exported in the terminal used to launch Airflow.

Airflow can then be started from that terminal with:

```bash
cd "$PROJECT_DIR"

export AIRFLOW_HOME="$HOME/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PROJECT_DIR/dags"

airflow standalone
```

The same `AIRFLOW_HOME` should be used when restarting Airflow to preserve its configuration, login credentials, and run history.

The Airflow UI is available at [localhost:8080](http://localhost:8080), using Airflow's generated credentials. The `research_library` DAG should be unpaused for scheduled runs. For Airflow 3's default Simple Auth Manager, the generated credentials can be viewed in another terminal with:

```bash
cat "$HOME/.airflow/simple_auth_manager_passwords.json.generated"
```

The DAG checks for new PDFs every **10 minutes**, using `schedule="*/10 * * * *"`. `catchup=False` prevents historical backfills, and `max_active_runs=1` prevents overlapping DAG runs. A run can also be triggered manually from the UI.

The pipeline:

1. Lists PDFs whose filenames are not already stored in SQLite.
2. Converts **all new PDFs**, sequentially, to local Markdown files.
3. Uses the local LLM to extract `title`, `summary`, `keywords`, and `category` from the first 4,000 Markdown characters of each paper.
4. Parses the JSON and validates required fields and basic types, retrying once if parsing or validation fails.
5. Saves the metadata in local SQLite, using unique filenames and storing keywords as a JSON string.
6. Regenerates the static HTML page from all saved records.
7. After successful HTML generation, sends one Discord notification containing the number of newly inserted papers. No notification is sent when no new papers were inserted.

Each task's logs in Airflow show progress and any errors. Running `python dags/paper_pipeline.py` loads the DAG definition; it does not execute the tasks.

Airflow, the rclone mount, and Ollama should remain running for scheduled processing. The schedule does not run while the computer is off.

## View the results

| Output | Location |
|---|---|
| Markdown | `$PROJECT_DIR/data/markdown/` |
| SQLite database, created automatically | `$PROJECT_DIR/data/papers.db` |
| Shared HTML library | `$DRIVE_MOUNT_DIR/output/index.html` |
| Processing notification | Discord channel selected for the webhook |

On Linux, the generated page can be opened locally with:

```bash
xdg-open "$DRIVE_MOUNT_DIR/output/index.html"
```

The search field filters titles, summaries, categories, keywords, and filenames as you type, ignoring case. Filtering runs in the browser using JavaScript; the static page does not query SQLite directly.

Refresh the page after a new pipeline run. Google Drive stores the HTML file; open it locally through the mount to use the page in your browser.

## Verify the workflow

Use these checks to validate the implementation and capture evidence for the report:

| Test | Expected result |
|---|---|
| Add two PDFs with new filenames and wait for a scheduled run | Both are converted, saved to SQLite, and listed in HTML |
| Check Discord after successful HTML generation | One notification reports the number of new papers |
| Run again without adding PDFs | No duplicate database records and no new notification |
| Search for a term present in one paper | Only matching papers remain visible |
| Search for `zzzz`, if absent from all papers | All paper cards are hidden |
| Clear the search field | All paper cards reappear |

Keep screenshots of a successful scheduled DAG run, the searchable HTML page, and the Discord notification for the report.

## Shared and local files

PDFs and the generated HTML page are shared through Google Drive. SQLite and Markdown stay local, outside the synced folder. Filenames are unique identifiers for papers already processed.