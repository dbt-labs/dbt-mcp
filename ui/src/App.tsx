import { useEffect, useMemo, useState } from "react";
import "./App.css";
import dbtLogoBLK from "../assets/dbt_logo BLK.svg";
import dbtLogoWHT from "../assets/dbt_logo WHT.svg";

type Project = {
  id: number;
  name: string;
  account_id: number;
  account_name: string;
};

type FetchRetryOptions = {
  attempts?: number;
  delayMs?: number;
  backoffFactor?: number;
  timeoutMs?: number;
  retryOnResponse?: (response: Response) => boolean;
};

function isAbortError(error: unknown): boolean {
  if (error instanceof DOMException) {
    return error.name === "AbortError";
  }
  return error instanceof Error && error.name === "AbortError";
}

function isNetworkError(error: unknown): boolean {
  if (error instanceof TypeError) {
    return true;
  }
  return error instanceof Error && error.name === "TypeError";
}

function sleep(ms: number) {
  return new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function fetchWithRetry(
  input: RequestInfo | URL,
  init?: RequestInit,
  options?: FetchRetryOptions,
): Promise<Response> {
  const {
    attempts = 3,
    delayMs = 500,
    backoffFactor = 2,
    timeoutMs = 10000,
    retryOnResponse,
  } = options ?? {};

  let currentDelay = delayMs;

  for (let attempt = 0; attempt < attempts; attempt++) {
    if (attempt > 0 && currentDelay > 0) {
      await sleep(currentDelay);
      currentDelay *= backoffFactor;
    }

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

    if (init?.signal) {
      init.signal.addEventListener("abort", () => controller.abort(), {
        once: true,
      });
    }

    try {
      const response = await fetch(input, {
        ...init,
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      if (
        retryOnResponse &&
        retryOnResponse(response) &&
        attempt < attempts - 1
      ) {
        try {
          await response.arrayBuffer();
        } catch {
          // Ignore - may already be consumed or reader locked
        }
        continue;
      }

      return response;
    } catch (error) {
      clearTimeout(timeoutId);

      if (isAbortError(error)) {
        throw error;
      }

      if (!isNetworkError(error)) {
        throw error;
      }

      if (attempt === attempts - 1) {
        throw error;
      }
    }
  }

  throw new Error("Failed to fetch after retries");
}

function parseHash(): URLSearchParams {
  const hash = window.location.hash.startsWith("#")
    ? window.location.hash.slice(1)
    : window.location.hash;
  const query = hash.startsWith("?") ? hash.slice(1) : hash;
  return new URLSearchParams(query);
}

type OAuthResult = {
  status: string | null;
  error: string | null;
  errorDescription: string | null;
};

function useOAuthResult(): OAuthResult {
  const params = useMemo(() => parseHash(), []);
  return {
    status: params.get("status"),
    error: params.get("error"),
    errorDescription: params.get("error_description"),
  };
}

export default function App() {
  const oauthResult = useOAuthResult();
  const [responseText, setResponseText] = useState<string | null>(null);
  const [projects, setProjects] = useState<Project[]>([]);
  const [projectsError, setProjectsError] = useState<string | null>(null);
  const [loadingProjects, setLoadingProjects] = useState(false);
  const [continuing, setContinuing] = useState(false);
  const [shutdownComplete, setShutdownComplete] = useState(false);
  const [selectedProjectIds, setSelectedProjectIds] = useState<Set<number>>(
    new Set(),
  );
  const [projectSearch, setProjectSearch] = useState("");

  // Load available projects after OAuth success
  useEffect(() => {
    if (oauthResult.status !== "success") return;
    const abortController = new AbortController();
    let cancelled = false;

    const loadProjects = async () => {
      setLoadingProjects(true);
      setProjectsError(null);

      try {
        const response = await fetchWithRetry(
          "/projects",
          { signal: abortController.signal },
          { attempts: 3, delayMs: 400 },
        );

        if (!response.ok) {
          throw new Error(`Failed to load projects (${response.status})`);
        }

        const data: Project[] = await response.json();

        if (!cancelled) {
          setProjects(data);
        }
      } catch (err) {
        if (cancelled || isAbortError(err)) {
          return;
        }

        const msg = err instanceof Error ? err.message : String(err);
        setProjectsError(msg);
      } finally {
        if (!cancelled) {
          setLoadingProjects(false);
        }
      }
    };

    loadProjects();

    return () => {
      cancelled = true;
      abortController.abort();
    };
  }, [oauthResult.status]);

  const toggleProject = (id: number) => {
    setSelectedProjectIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) {
        next.delete(id);
      } else {
        next.add(id);
      }
      return next;
    });
  };

  const onContinue = async () => {
    if (continuing || selectedProjectIds.size === 0) return;
    const firstProject = projects.find((p) => selectedProjectIds.has(p.id));
    if (!firstProject) return;

    setContinuing(true);
    setResponseText(null);

    try {
      const selectRes = await fetchWithRetry(
        "/selected_projects",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            account_id: firstProject.account_id,
            project_ids: Array.from(selectedProjectIds),
          }),
        },
        { attempts: 3, delayMs: 400 },
      );

      if (!selectRes.ok) {
        setResponseText(await selectRes.text());
        return;
      }

      const shutdownRes = await fetchWithRetry(
        "/shutdown",
        { method: "POST" },
        { attempts: 3, delayMs: 400 },
      );

      if (shutdownRes.ok) {
        setShutdownComplete(true);
        window.close();
      } else {
        setResponseText(await shutdownRes.text());
      }
    } catch (err) {
      if (isNetworkError(err)) {
        setResponseText(
          "Something went wrong when setting up the authentication. Please close this window and try again.",
        );
      } else {
        setResponseText(String(err));
      }
    } finally {
      setContinuing(false);
    }
  };

  const filteredProjects = useMemo(() => {
    const q = projectSearch.trim().toLowerCase();
    if (!q) return projects;
    return projects.filter((p) =>
      `${p.name} ${p.account_name}`.toLowerCase().includes(q),
    );
  }, [projects, projectSearch]);

  return (
    <div className="app-container">
      <div className="logo-container">
        <img src={dbtLogoBLK} alt="dbt" className="logo logo-light" />
        <img src={dbtLogoWHT} alt="dbt" className="logo logo-dark" />
      </div>
      <div className="app-content">
        <header className="app-header">
          <h1>dbt MCP Server — Authentication</h1>
          <p>
            The dbt MCP server needs to authenticate with dbt Platform via
            OAuth.
          </p>
        </header>

        {oauthResult.status === "error" && (
          <section className="error-section">
            <div className="section-header">
              <h2>Authentication Error</h2>
              <p>The dbt MCP server could not authenticate with dbt Platform</p>
            </div>

            <div className="error-details">
              {oauthResult.error && (
                <div className="error-item">
                  <strong>Error Code:</strong>
                  <code className="error-code">{oauthResult.error}</code>
                </div>
              )}

              {oauthResult.errorDescription && (
                <div className="error-item">
                  <strong>Description:</strong>
                  <p className="error-description">
                    {decodeURIComponent(oauthResult.errorDescription)}
                  </p>
                </div>
              )}

              <div className="error-actions">
                <p>
                  Please close this window and try again. If the problem
                  persists, contact support.
                </p>
              </div>
            </div>
          </section>
        )}

        {oauthResult.status === "success" && !shutdownComplete && (
          <section className="project-selection-section">
            <div className="section-header">
              <h2>Select Projects</h2>
              <p>Choose the dbt projects you want to work with</p>
            </div>

            <div className="form-content">
              {loadingProjects && (
                <div className="loading-state">
                  <div className="spinner"></div>
                  <span>Loading projects…</span>
                </div>
              )}

              {projectsError && (
                <div className="error-state">
                  <strong>Error loading projects</strong>
                  <p>{projectsError}</p>
                </div>
              )}

              {!loadingProjects && !projectsError && (
                <div className="form-group">
                  <div className="project-checklist">
                    <div className="dropdown-search-wrapper">
                      <input
                        type="text"
                        className="dropdown-search"
                        aria-label="Search projects"
                        placeholder="Search projects..."
                        value={projectSearch}
                        onChange={(e) => setProjectSearch(e.target.value)}
                      />
                    </div>
                    {filteredProjects.map((p) => (
                      <label key={p.id} className="project-checkbox-row">
                        <input
                          type="checkbox"
                          checked={selectedProjectIds.has(p.id)}
                          onChange={() => toggleProject(p.id)}
                        />
                        <div>
                          <div className="option-primary">{p.name}</div>
                          <div className="option-secondary">
                            {p.account_name}
                          </div>
                        </div>
                      </label>
                    ))}
                    {filteredProjects.length === 0 && (
                      <div className="dropdown-no-results">
                        No results found
                      </div>
                    )}
                  </div>

                  <div className="button-container" style={{ marginTop: "1rem" }}>
                    <button
                      onClick={onContinue}
                      className="primary-button"
                      disabled={continuing || selectedProjectIds.size === 0}
                    >
                      {continuing
                        ? "Setting up…"
                        : `Continue${selectedProjectIds.size > 0 ? ` (${selectedProjectIds.size} project${selectedProjectIds.size !== 1 ? "s" : ""})` : ""}`}
                    </button>
                  </div>
                </div>
              )}
            </div>
          </section>
        )}

        {shutdownComplete && (
          <section className="completion-section">
            <div className="completion-card">
              <h2>All Set!</h2>
              <p>
                The dbt MCP server is authenticated and configured with your dbt
                Platform account. This window can now be closed.
              </p>
            </div>
          </section>
        )}

        {responseText && (
          <section className="response-section">
            <div className="section-header">
              <h3>Response</h3>
            </div>
            <pre className="response-text">{responseText}</pre>
          </section>
        )}
      </div>
    </div>
  );
}
