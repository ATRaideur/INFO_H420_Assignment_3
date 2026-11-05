# Research Library — INFO-H420 Assignment 3

A scheduled Airflow pipeline that reads new PDFs from a shared Google Drive folder, converts them to Markdown, extracts metadata using a local LLM, stores it in SQLite, generates a searchable static HTML library, and sends Discord notifications.

All pipeline features required by the assignment are implemented.

## Requirements

Python packages: `apache-airflow`, `pymupdf4llm`, `openai`, `requests`, and `pendulum`. Use the same tested Python and Airflow versions as the project owner; the code uses Airflow 3's `airflow.sdk` API.

`sqlite3`, `json`, `pathlib`, `html`, and `os` are included with Python. The repository must also contain `metadata_utils.py`, which provides `validate_metadata`.

External tools:

- **rclone** for access to the shared Google Drive folder.
- **Ollama**, serving `qwen2.5:3b` at `http://localhost:11434/v1`.
- A **Discord webhook** targeting the notification channel.

Activate the Python environment containing these packages before running Airflow. Its location depends on your local setup and does not need to be inside the repository. For a fresh installation, follow the [official Airflow installation instructions](https://airflow.apache.org/docs/apache-airflow/stable/start.html) using the project owner's tested version and the matching Python constraints.

## Before starting

The project owner provides:

- GitHub repository URL.
- Shared `ResearchLibrary` Drive folder link, with Editor access.
- OAuth Client ID and client secret, sent privately.
- Discord webhook URL, sent privately, or access to configure your own webhook.

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

Sign in with your own Google account. Each member keeps their authentication tokens locally; never share an authenticated `rclone.conf`.

Scope `1` grants broad access to the authenticated account's Drive. Mounting only the project folder does not narrow that authorization. A dedicated project account can keep personal files separate.

## Mount the shared folder

Open the shared **ResearchLibrary** folder itself in your browser. Its folder ID is the value after `/folders/` in the URL; do not use the ID of its parent folder.

Create the local mount directory:

```bash
mkdir -p ~/Documents/ResearchLibraryDrive
```

Run the following command, paste the actual folder ID when prompted, and press Enter:

```bash
read -r -p "Paste your ResearchLibrary folder ID: " drive_folder_id
```

Then mount the folder:

```bash
rclone mount gdrive: "$HOME/Documents/ResearchLibraryDrive" \
  --drive-root-folder-id "$drive_folder_id" \
  --vfs-cache-mode full
```

Keep this terminal running. In another terminal, verify:

```bash
ls ~/Documents/ResearchLibraryDrive
ls ~/Documents/ResearchLibraryDrive/papers
```

The mounted folder should contain `papers` and `output`. Add input PDFs with unique filenames and the `.pdf` extension to `papers`. Rclone fetches and caches files locally when they are accessed and uploads generated HTML changes to Drive.

Mounting by folder ID also works for members who have access through a shared link without having the same parent-folder path in their own Drive.

## Configure Discord notifications

Create a webhook in your Discord server under **Server Settings → Integrations → Webhooks**, select the destination text channel, and copy its URL. Alternatively, use the webhook provided privately by the project owner.

In the terminal that will launch Airflow, run this command, paste the URL when prompted, and press Enter. The input is hidden:

```bash
read -r -s -p "Paste Discord webhook URL, then press Enter: " DISCORD_WEBHOOK_URL
```

Export the variable:

```bash
export DISCORD_WEBHOOK_URL
```

The DAG reads `DISCORD_WEBHOOK_URL` from the environment. Set it before launching Airflow and repeat this step when starting from a new terminal. To change the URL, stop Airflow, set the new value in its launch terminal, and restart Airflow. Do not commit the URL to Git.

## Run the pipeline

Ensure Ollama is running with `qwen2.5:3b` available. Keep the rclone mount active. In the terminal where you configured the webhook, activate your Python environment containing the required packages, then run:

```bash
cd ~/Documents/research-library

export AIRFLOW_HOME="$HOME/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"

airflow standalone
```

Keep the same `AIRFLOW_HOME` when restarting Airflow so that it uses the same configuration, login credentials, and run history.

Open [localhost:8080](http://localhost:8080), sign in using Airflow's generated credentials, and unpause the `research_library` DAG. For Airflow 3's default Simple Auth Manager, retrieve the generated credentials in another terminal with:

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

Inspect each task's logs in Airflow for progress or errors. Running `python dags/paper_pipeline.py` loads the DAG definition; it does not execute the tasks.

Airflow, the rclone mount, and Ollama must remain running for scheduled processing. The schedule does not run while the computer is off.

## View the results

| Output | Location |
|---|---|
| Markdown | `~/Documents/research-library/data/markdown/` |
| SQLite database, created automatically | `~/Documents/research-library/data/papers.db` |
| Shared HTML library | `~/Documents/ResearchLibraryDrive/output/index.html` |
| Processing notification | Discord channel selected for the webhook |

Open the page locally:

```bash
xdg-open "$HOME/Documents/ResearchLibraryDrive/output/index.html"
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

Use one designated machine to run the pipeline and publish the shared HTML so that the page consistently reflects that machine's database.

Keep OAuth credentials, authentication tokens, Discord webhook URLs, `.env`, virtual environments, Airflow runtime files, and generated local data outside Git.

