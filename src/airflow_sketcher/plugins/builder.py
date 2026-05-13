import json
import os
import re
from copy import deepcopy
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel

from airflow_sketcher.dag_factory import register_airflow_sketcher_importer
from airflow.models.dagbag import DagBag
from airflow.plugins_manager import AirflowPlugin
from airflow.settings import DAGS_FOLDER


PLUGIN_URL_PREFIX = "/airflow-sketcher"
PLUGIN_ICON_LIGHT_PATH = "/icon.svg"
PLUGIN_ICON_DARK_PATH = "/icon-dark.svg"
DEFAULT_FILE_EXTENSION = ".excalidraw"
SAFE_FILENAME_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")
DAGS_DIRECTORY = Path(DAGS_FOLDER).resolve()
SCENE_SOURCE = "airflow-sketcher-builder"
DAG_SOURCE_PARAM_KEY = "excalidraw_source_file"
DEFAULT_SCENE_APP_STATE = {
    "viewBackgroundColor": "#f7f1e8",
    "gridSize": 20,
    "gridStep": 5,
}
LOADED_SCENE_APP_STATE_KEYS = {
    "gridModeEnabled",
    "gridSize",
    "gridStep",
    "scrollX",
    "scrollY",
    "theme",
    "viewBackgroundColor",
    "viewModeEnabled",
    "zoom",
}


register_airflow_sketcher_importer()


class SaveFileRequest(BaseModel):
    filename: str
    content: dict


def normalize_filename(filename: str) -> str:
    cleaned = filename.strip()
    if not cleaned:
        raise HTTPException(status_code=400, detail="Filename is required")

    if not cleaned.endswith(DEFAULT_FILE_EXTENSION):
        cleaned = f"{cleaned}{DEFAULT_FILE_EXTENSION}"

    if not SAFE_FILENAME_PATTERN.fullmatch(cleaned):
        raise HTTPException(
            status_code=400,
            detail="Filename may only contain letters, numbers, dots, dashes, and underscores",
        )

    return cleaned


def resolve_dag_file_path(filename: str) -> Path:
    normalized = normalize_filename(filename)
    file_path = (DAGS_DIRECTORY / normalized).resolve()
    if file_path.parent != DAGS_DIRECTORY:
        raise HTTPException(status_code=400, detail="Invalid filename")
    return file_path


def build_empty_scene() -> dict:
    return {
        "type": "excalidraw",
        "version": 2,
        "source": SCENE_SOURCE,
        "elements": [],
        "appState": deepcopy(DEFAULT_SCENE_APP_STATE),
        "files": {},
    }


def extract_dag_attributes_from_scene(payload: dict, fallback_filename: str) -> dict:
    dag_attrs = {}
    for element in payload.get("elements", []):
        if element.get("type") != "text":
            continue

        text = element.get("text", "")
        if not text.startswith("dag:"):
            continue

        for line in text.splitlines()[1:]:
            if "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()

            if key in {"dag_id", "schedule"}:
                dag_attrs[key] = value
        break

    dag_attrs.setdefault("dag_id", Path(fallback_filename).stem)
    dag_attrs.setdefault("schedule", None)
    return dag_attrs


def get_dag_source_from_params(dag_id: str) -> str | None:
    try:
        dag_bag = DagBag(read_dags_from_db=True)
        dag = dag_bag.get_dag(dag_id)
    except Exception:
        return None

    if dag is None:
        return None

    params = getattr(dag, "params", None)
    if not params:
        return None

    source = params.get(DAG_SOURCE_PARAM_KEY)
    source = getattr(source, "value", source)
    if source in (None, ""):
        return None

    try:
        normalized_source = normalize_filename(str(source))
    except HTTPException:
        return None

    return normalized_source if resolve_dag_file_path(normalized_source).exists() else None


def find_excalidraw_file_for_dag(dag_id: str) -> str | None:
    source_from_params = get_dag_source_from_params(dag_id)
    if source_from_params is not None:
        return source_from_params

    for file_path in sorted(DAGS_DIRECTORY.glob(f"*{DEFAULT_FILE_EXTENSION}")):
        try:
            payload = json.loads(file_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue

        dag_attrs = extract_dag_attributes_from_scene(payload, file_path.name)
        if dag_attrs.get("dag_id") == dag_id:
            return file_path.name

    return None


def build_plugin_icon_svg(*, background: str, stroke: str) -> str:
    return f"""
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64" fill="none">
  <rect x="6" y="6" width="52" height="52" rx="16" fill="{background}" />
  <path d="M20 42 L32 22 L44 34" stroke="{stroke}" stroke-width="4" stroke-linecap="round" stroke-linejoin="round" />
  <circle cx="20" cy="42" r="4" fill="{stroke}" />
  <circle cx="32" cy="22" r="4" fill="{stroke}" />
  <circle cx="44" cy="34" r="4" fill="{stroke}" />
  <path d="M24 48 H40" stroke="{stroke}" stroke-width="3" stroke-linecap="round" opacity="0.85" />
</svg>
""".strip()


def sanitize_loaded_app_state(app_state: dict | None) -> dict:
    if not isinstance(app_state, dict):
        return deepcopy(DEFAULT_SCENE_APP_STATE)

    sanitized = deepcopy(DEFAULT_SCENE_APP_STATE)
    for key in LOADED_SCENE_APP_STATE_KEYS:
        value = app_state.get(key)
        if value is not None:
            sanitized[key] = value
    return sanitized


def normalize_scene_payload(payload: dict) -> dict:
    scene = build_empty_scene()
    scene.update(payload)
    scene["type"] = "excalidraw"
    scene["version"] = payload.get("version", 2)
    scene["source"] = payload.get("source", SCENE_SOURCE)
    scene["elements"] = payload.get("elements", [])
    scene["appState"] = sanitize_loaded_app_state(payload.get("appState"))
    scene["files"] = payload.get("files", {})
    return scene


def render_html() -> str:
    return """
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Airflow Sketcher</title>
    <link rel="preconnect" href="https://fonts.googleapis.com" />
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
    <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@400;500;600&display=swap" rel="stylesheet" />
    <link rel="stylesheet" href="https://esm.sh/@excalidraw/excalidraw@0.18.0/dist/dev/index.css" />
    <script>
      window.EXCALIDRAW_ASSET_PATH = "https://esm.sh/@excalidraw/excalidraw@0.18.0/dist/prod/";
    </script>
    <script type="importmap">
      {
        "imports": {
          "react": "https://esm.sh/react@19.0.0",
          "react/jsx-runtime": "https://esm.sh/react@19.0.0/jsx-runtime",
          "react-dom": "https://esm.sh/react-dom@19.0.0",
          "react-dom/client": "https://esm.sh/react-dom@19.0.0/client"
        }
      }
    </script>
    <style>
      :root {
        --bg: #f4efe6;
        --panel: rgba(255, 252, 247, 0.94);
        --panel-strong: #fffaf2;
        --line: rgba(48, 34, 22, 0.12);
        --text: #281d14;
        --muted: #6d5a49;
        --accent: #b65f33;
        --accent-strong: #8e4722;
        --shadow: 0 24px 50px rgba(58, 35, 16, 0.12);
      }

      * {
        box-sizing: border-box;
      }

      html,
      body,
      #root {
        height: 100%;
        margin: 0;
      }

      body {
        background:
          radial-gradient(circle at top left, rgba(255, 215, 173, 0.55), transparent 28%),
          radial-gradient(circle at bottom right, rgba(217, 165, 112, 0.30), transparent 25%),
          linear-gradient(180deg, #f6f0e8 0%, #f1ebdf 100%);
        color: var(--text);
        font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      }

      .builder-shell {
        display: grid;
        grid-template-rows: auto 1fr;
        gap: 14px;
        height: 100%;
        padding: 18px;
      }

      .topbar {
        display: grid;
        grid-template-columns: minmax(0, 1.4fr) minmax(0, 0.9fr) auto;
        gap: 12px;
        padding: 14px;
        border: 1px solid var(--line);
        border-radius: 18px;
        background: var(--panel);
        box-shadow: var(--shadow);
        backdrop-filter: blur(14px);
        align-items: end;
      }

      .field {
        display: grid;
        gap: 6px;
      }

      .field label,
      .status {
        color: var(--muted);
        font-size: 12px;
        font-weight: 500;
        letter-spacing: 0.06em;
        text-transform: uppercase;
      }

      .field input,
      .field select {
        width: 100%;
        height: 42px;
        border: 1px solid var(--line);
        border-radius: 12px;
        background: var(--panel-strong);
        color: var(--text);
        padding: 0 12px;
        font: inherit;
      }

      .actions {
        display: flex;
        gap: 10px;
        align-items: center;
        justify-content: flex-end;
        flex-wrap: wrap;
      }

      .button {
        height: 42px;
        border: 0;
        border-radius: 999px;
        padding: 0 16px;
        cursor: pointer;
        font: inherit;
        font-weight: 600;
        transition: transform 120ms ease, opacity 120ms ease, background 120ms ease;
      }

      .button:hover {
        transform: translateY(-1px);
      }

      .button:disabled {
        cursor: not-allowed;
        opacity: 0.55;
        transform: none;
      }

      .button-primary {
        background: linear-gradient(135deg, var(--accent) 0%, var(--accent-strong) 100%);
        color: #fffaf4;
      }

      .button-secondary {
        background: rgba(255, 250, 242, 0.9);
        color: var(--text);
        border: 1px solid var(--line);
      }

      .status {
        min-height: 18px;
        text-align: right;
      }

      .canvas-panel {
        min-height: 0;
        border: 1px solid var(--line);
        border-radius: 22px;
        overflow: hidden;
        background: rgba(255, 252, 247, 0.88);
        box-shadow: var(--shadow);
      }

      .canvas-panel > div {
        height: 100%;
      }

      .boot-state {
        display: grid;
        place-items: center;
        min-height: 100vh;
        padding: 24px;
      }

      .boot-card {
        max-width: 680px;
        padding: 22px 24px;
        border: 1px solid var(--line);
        border-radius: 18px;
        background: var(--panel);
        box-shadow: var(--shadow);
      }

      .boot-title {
        margin: 0 0 8px;
        font-size: 18px;
        font-weight: 600;
      }

      .boot-copy {
        margin: 0;
        color: var(--muted);
        line-height: 1.5;
      }

      .boot-error {
        margin-top: 14px;
        padding: 12px 14px;
        border-radius: 12px;
        background: rgba(182, 95, 51, 0.08);
        color: var(--accent-strong);
        font-family: "IBM Plex Mono", monospace;
        font-size: 12px;
        white-space: pre-wrap;
        word-break: break-word;
      }

      .footnote {
        margin-top: 2px;
        color: var(--muted);
        font-family: "IBM Plex Mono", monospace;
        font-size: 12px;
      }

      @media (max-width: 980px) {
        .topbar {
          grid-template-columns: 1fr;
          align-items: stretch;
        }

        .actions {
          justify-content: flex-start;
        }

        .status {
          text-align: left;
        }
      }
    </style>
  </head>
  <body>
    <div id="root">
      <div class="boot-state">
        <div class="boot-card">
          <h1 class="boot-title">Loading Excalidraw editor</h1>
          <p class="boot-copy">The page is starting the editor and connecting it to the DAGs folder.</p>
        </div>
      </div>
    </div>
    <script type="module">
      const rootElement = document.getElementById("root");

      const renderBootError = (message) => {
        rootElement.innerHTML = `
          <div class="boot-state">
            <div class="boot-card">
              <h1 class="boot-title">Excalidraw failed to load</h1>
              <p class="boot-copy">The page loaded, but the editor could not start in the browser.</p>
              <div class="boot-error">${String(message)}</div>
            </div>
          </div>
        `;
      };

      const bootstrap = async () => {
        try {
          if (!rootElement) {
            throw new Error("Missing root element: #root");
          }

          const React = await import("react");
          const ReactDOMClient = await import("react-dom/client");
          const ExcalidrawModule = await import(
            "https://esm.sh/@excalidraw/excalidraw@0.18.0/dist/dev/index.js?external=react,react-dom"
          );

          const { useEffect, useRef, useState } = React;
          const { createRoot } = ReactDOMClient;
          const { Excalidraw } = ExcalidrawModule;
          const API_BASE = window.location.pathname.endsWith("/")
            ? window.location.pathname.slice(0, -1)
            : window.location.pathname;
          const urlParams = new URLSearchParams(window.location.search);
          const EMPTY_SCENE_TEMPLATE = __EMPTY_SCENE_TEMPLATE__;
          const cloneSceneTemplate = (value) => JSON.parse(JSON.stringify(value));
          const createEmptyScene = () => cloneSceneTemplate(EMPTY_SCENE_TEMPLATE);
          const initialFilename = normalizeFilename(urlParams.get("filename") || "");
          const initialDagId = (urlParams.get("dag_id") || "").trim();

          function normalizeFilename(filename) {
            const trimmed = filename.trim();
            if (!trimmed) {
              return "";
            }
            return trimmed.endsWith(".excalidraw") ? trimmed : `${trimmed}.excalidraw`;
          }

          function BuilderApp() {
            const [availableFiles, setAvailableFiles] = useState([]);
            const [selectedFile, setSelectedFile] = useState("");
            const [filename, setFilename] = useState("untitled.excalidraw");
            const [status, setStatus] = useState("Ready");
            const [loading, setLoading] = useState(false);
            const [saving, setSaving] = useState(false);
            const [excalidrawApi, setExcalidrawApi] = useState(null);
            const [pendingAutoloadFile, setPendingAutoloadFile] = useState("");
            const latestSceneRef = useRef(createEmptyScene());

            const refreshFiles = async () => {
              const response = await fetch(`${API_BASE}/api/files`);
              if (!response.ok) {
                throw new Error("Unable to refresh files");
              }
              const payload = await response.json();
              setAvailableFiles(payload.files);
              return payload.files;
            };

            useEffect(() => {
              let isCancelled = false;

              const initialize = async () => {
                try {
                  await refreshFiles();

                  let targetFilename = initialFilename;
                  if (!targetFilename && initialDagId) {
                    const response = await fetch(`${API_BASE}/api/dag-source/${encodeURIComponent(initialDagId)}`);
                    if (response.ok) {
                      const payload = await response.json();
                      targetFilename = payload.filename;
                    } else if (response.status !== 404) {
                      throw new Error(`Unable to resolve source diagram for ${initialDagId}`);
                    }
                  }

                  if (isCancelled) {
                    return;
                  }

                  if (!targetFilename) {
                    setStatus(initialDagId ? `No source diagram is linked to ${initialDagId}` : "Ready");
                    return;
                  }

                  setSelectedFile(targetFilename);
                  setPendingAutoloadFile(targetFilename);
                } catch (error) {
                  if (!isCancelled) {
                    setStatus(error.message);
                  }
                }
              };

              void initialize();

              return () => {
                isCancelled = true;
              };
            }, []);

            const applyScene = (scene, nextFilename) => {
              const nextScene = {
                ...createEmptyScene(),
                ...scene,
                elements: scene.elements || [],
                appState: scene.appState || cloneSceneTemplate(EMPTY_SCENE_TEMPLATE.appState),
                files: scene.files || {},
              };

              latestSceneRef.current = nextScene;
              setFilename(nextFilename || "untitled.excalidraw");
              if (!excalidrawApi) {
                return;
              }

              excalidrawApi.resetScene({ resetLoadingState: true });
              excalidrawApi.updateScene({
                elements: nextScene.elements,
                appState: nextScene.appState,
              });

              if (typeof excalidrawApi.addFiles === "function") {
                const files = Object.values(nextScene.files || {});
                if (files.length > 0) {
                  excalidrawApi.addFiles(files);
                }
              }

              if (excalidrawApi.history && typeof excalidrawApi.history.clear === "function") {
                excalidrawApi.history.clear();
              }

              if (typeof excalidrawApi.scrollToContent === "function") {
                excalidrawApi.scrollToContent(undefined, { fitToContent: true });
              }
            };

            useEffect(() => {
              if (!excalidrawApi) {
                return;
              }

              applyScene(latestSceneRef.current, filename);
            }, [excalidrawApi]);

            useEffect(() => {
              if (!pendingAutoloadFile) {
                return;
              }

              let isCancelled = false;

              const autoload = async () => {
                setLoading(true);
                setStatus(`Loading ${pendingAutoloadFile}`);

                try {
                  await loadSceneByFilename(pendingAutoloadFile);
                } catch (error) {
                  if (!isCancelled) {
                    setStatus(error.message);
                  }
                } finally {
                  if (!isCancelled) {
                    setLoading(false);
                    setPendingAutoloadFile("");
                  }
                }
              };

              void autoload();

              return () => {
                isCancelled = true;
              };
            }, [pendingAutoloadFile]);

            const loadSceneByFilename = async (requestedFilename) => {
              const response = await fetch(`${API_BASE}/api/files/${encodeURIComponent(requestedFilename)}`);
              if (!response.ok) {
                throw new Error(`Unable to load ${requestedFilename}`);
              }

              const payload = await response.json();
              applyScene(payload.content, payload.filename);
              setSelectedFile(payload.filename);
              setStatus(`Loaded ${payload.filename}`);
              return payload;
            };

            const handleNew = () => {
              setSelectedFile("");
              applyScene(createEmptyScene(), "untitled.excalidraw");
              setStatus("Started a new diagram");
            };

            const handleLoad = async () => {
              if (!selectedFile) {
                setStatus("Choose a file to load");
                return;
              }

              setLoading(true);
              setStatus(`Loading ${selectedFile}`);

              try {
                await loadSceneByFilename(selectedFile);
              } catch (error) {
                setStatus(error.message);
              } finally {
                setLoading(false);
              }
            };

            const handleSave = async () => {
              const normalizedFilename = normalizeFilename(filename);
              if (!normalizedFilename) {
                setStatus("Enter a filename before saving");
                return;
              }

              if (!excalidrawApi) {
                setStatus("Editor is still starting");
                return;
              }

              const content = {
                ...createEmptyScene(),
                elements: typeof excalidrawApi.getSceneElementsIncludingDeleted === "function"
                  ? excalidrawApi.getSceneElementsIncludingDeleted()
                  : excalidrawApi.getSceneElements(),
                appState: excalidrawApi.getAppState(),
                files: typeof excalidrawApi.getFiles === "function" ? excalidrawApi.getFiles() : {},
              };

              setSaving(true);
              setStatus(`Saving ${normalizedFilename}`);

              try {
                const response = await fetch(`${API_BASE}/api/files`, {
                  method: "POST",
                  headers: {
                    "Content-Type": "application/json",
                  },
                  body: JSON.stringify({
                    filename: normalizedFilename,
                    content,
                  }),
                });

                if (!response.ok) {
                  const errorPayload = await response.json().catch(() => ({ detail: "Unable to save file" }));
                  throw new Error(errorPayload.detail || "Unable to save file");
                }

                const payload = await response.json();
                latestSceneRef.current = content;
                setFilename(payload.filename);
                setSelectedFile(payload.filename);
                await refreshFiles();
                setStatus(`Saved ${payload.filename} to the DAGs folder`);
              } catch (error) {
                setStatus(error.message);
              } finally {
                setSaving(false);
              }
            };

            return React.createElement(
              "div",
              { className: "builder-shell" },
              React.createElement(
                "div",
                { className: "topbar" },
                React.createElement(
                  "div",
                  { className: "field" },
                  React.createElement("label", null, "Diagram File"),
                  React.createElement("input", {
                    value: filename,
                    onChange: (event) => setFilename(event.target.value),
                    placeholder: "my_pipeline.excalidraw",
                  }),
                  React.createElement(
                    "div",
                    { className: "footnote" },
                    "Files are written directly into /opt/airflow/dags and picked up by your Excalidraw DAG generator."
                  )
                ),
                React.createElement(
                  "div",
                  { className: "field" },
                  React.createElement("label", null, "Existing Files"),
                  React.createElement(
                    "select",
                    {
                      value: selectedFile,
                      onChange: (event) => setSelectedFile(event.target.value),
                    },
                    React.createElement("option", { value: "" }, "Select a saved diagram"),
                    availableFiles.map((entry) => React.createElement(
                      "option",
                      { key: entry.name, value: entry.name },
                      entry.name
                    ))
                  ),
                  React.createElement("div", { className: "footnote" }, `${availableFiles.length} diagram${availableFiles.length === 1 ? "" : "s"} available`)
                ),
                React.createElement(
                  "div",
                  null,
                  React.createElement(
                    "div",
                    { className: "actions" },
                    React.createElement(
                      "button",
                      { className: "button button-secondary", onClick: handleNew, type: "button" },
                      "New"
                    ),
                    React.createElement(
                      "button",
                      {
                        className: "button button-secondary",
                        onClick: handleLoad,
                        type: "button",
                        disabled: loading || !selectedFile,
                      },
                      loading ? "Loading..." : "Load"
                    ),
                    React.createElement(
                      "button",
                      {
                        className: "button button-primary",
                        onClick: handleSave,
                        type: "button",
                        disabled: saving,
                      },
                      saving ? "Saving..." : "Save"
                    )
                  ),
                  React.createElement("div", { className: "status" }, status)
                )
              ),
              React.createElement(
                "div",
                { className: "canvas-panel" },
                React.createElement(
                  "div",
                  null,
                  React.createElement(Excalidraw, {
                    excalidrawAPI: setExcalidrawApi,
                    initialData: latestSceneRef.current,
                    onChange: (elements, appState, files) => {
                      latestSceneRef.current = {
                        ...latestSceneRef.current,
                        elements,
                        appState,
                        files,
                      };
                    },
                  })
                )
              )
            );
          }

          createRoot(rootElement).render(React.createElement(BuilderApp));
        } catch (error) {
          console.error(error);
          renderBootError(error && error.stack ? error.stack : error);
        }
      };

      bootstrap();
    </script>
  </body>
</html>
    """.replace("__EMPTY_SCENE_TEMPLATE__", json.dumps(build_empty_scene()))


app = FastAPI()


@app.get("/", response_class=HTMLResponse)
async def excalidraw_builder_page():
    return HTMLResponse(render_html())


@app.get(PLUGIN_ICON_LIGHT_PATH)
async def excalidraw_builder_icon() -> Response:
    return Response(
        content=build_plugin_icon_svg(background="#fff3de", stroke="#8e4722"),
        media_type="image/svg+xml",
        headers={"Cache-Control": "public, max-age=3600"},
    )


@app.get(PLUGIN_ICON_DARK_PATH)
async def excalidraw_builder_icon_dark() -> Response:
    return Response(
        content=build_plugin_icon_svg(background="#2a2019", stroke="#f4c39d"),
        media_type="image/svg+xml",
        headers={"Cache-Control": "public, max-age=3600"},
    )


@app.get("/api/files")
async def list_excalidraw_files():
    files = []
    for file_path in sorted(DAGS_DIRECTORY.glob(f"*{DEFAULT_FILE_EXTENSION}")):
        stat = file_path.stat()
        files.append(
            {
                "name": file_path.name,
                "modified": stat.st_mtime,
                "size": stat.st_size,
            }
        )
    return {"files": files}


@app.get("/api/files/{filename}")
async def load_excalidraw_file(filename: str):
    file_path = resolve_dag_file_path(filename)
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")

    try:
        content = json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid Excalidraw JSON: {exc}") from exc

    return {"filename": file_path.name, "content": normalize_scene_payload(content)}


@app.get("/api/dag-source/{dag_id}")
async def get_excalidraw_source_for_dag(dag_id: str):
  filename = find_excalidraw_file_for_dag(dag_id)
  if filename is None:
    raise HTTPException(status_code=404, detail="No Excalidraw source file found for DAG")

  return {"dag_id": dag_id, "filename": filename}


@app.post("/api/files")
async def save_excalidraw_file(payload: SaveFileRequest):
    file_path = resolve_dag_file_path(payload.filename)
    normalized_content = normalize_scene_payload(payload.content)
    file_path.write_text(json.dumps(normalized_content, indent=2), encoding="utf-8")
    return {"filename": file_path.name, "path": os.fspath(file_path)}


class AirflowSketcherPlugin(AirflowPlugin):
    name = "airflow_sketcher"
    fastapi_apps = [
        {
            "app": app,
            "url_prefix": PLUGIN_URL_PREFIX,
            "name": "Airflow Sketcher API",
        }
    ]
    external_views = [
        {
          "name": "Airflow Sketcher",
          "href": f"{PLUGIN_URL_PREFIX}/",
          "icon": f"{PLUGIN_URL_PREFIX}{PLUGIN_ICON_LIGHT_PATH}",
          "icon_dark_mode": f"{PLUGIN_URL_PREFIX}{PLUGIN_ICON_DARK_PATH}",
          "url_route": "dag-builder",
        },
        {
          "name": "Builder",
          "href": f"{PLUGIN_URL_PREFIX}/?dag_id={{DAG_ID}}",
          "icon": f"{PLUGIN_URL_PREFIX}{PLUGIN_ICON_LIGHT_PATH}",
          "icon_dark_mode": f"{PLUGIN_URL_PREFIX}{PLUGIN_ICON_DARK_PATH}",
          "url_route": "dag-builder-source",
          "destination": "dag",
        },
    ]