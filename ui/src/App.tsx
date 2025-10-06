import { useEffect, useMemo, useState, useRef } from "react";
import "./App.css";
import dbtLogoBLK from "../assets/dbt_logo BLK.svg";
import dbtLogoWHT from "../assets/dbt_logo WHT.svg";

type Project = {
  id: number;
  name: string;
  account_id: number;
  account_name: string;
};

type DbtPlatformContext = {
  dev_environment: {
    id: number;
    name: string;
    deployment_type: string;
  } | null;
  prod_environment: {
    id: number;
    name: string;
    deployment_type: string;
  } | null;
  decoded_access_token: {
    decoded_claims: {
      sub: number;
    };
  };
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
  options?: FetchRetryOptions
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

    // Listen to existing signal if present
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
        // Consume response body to free resources
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

type CustomDropdownProps = {
  value: number | null;
  onChange: (value: string) => void;
  options: Project[];
  placeholder: string;
  id: string;
};

function CustomDropdown({
  value,
  onChange,
  options,
  placeholder,
  id,
}: CustomDropdownProps) {
  const [isOpen, setIsOpen] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);

  // Close dropdown when clicking outside
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (
        dropdownRef.current &&
        !dropdownRef.current.contains(event.target as Node)
      ) {
        setIsOpen(false);
      }
    }

    if (isOpen) {
      document.addEventListener("mousedown", handleClickOutside);
      return () => {
        document.removeEventListener("mousedown", handleClickOutside);
      };
    }
  }, [isOpen]);

  // Handle keyboard navigation
  useEffect(() => {
    function handleKeyDown(event: KeyboardEvent) {
      if (!isOpen) {
        if (
          event.key === "Enter" ||
          event.key === " " ||
          event.key === "ArrowDown"
        ) {
          event.preventDefault();
          setIsOpen(true);
        }
        return;
      }

      if (event.key === "Escape") {
        setIsOpen(false);
        triggerRef.current?.focus();
      }
    }

    if (triggerRef.current?.contains(document.activeElement)) {
      document.addEventListener("keydown", handleKeyDown);
      return () => {
        document.removeEventListener("keydown", handleKeyDown);
      };
    }
  }, [isOpen]);

  const selectedProject = options.find((p) => p.id === value);

  const handleToggle = () => {
    setIsOpen(!isOpen);
  };

  const handleOptionSelect = (project: Project) => {
    onChange(project.id.toString());
    setIsOpen(false);
    triggerRef.current?.focus();
  };

  return (
    <div className="custom-dropdown" ref={dropdownRef}>
      <button
        ref={triggerRef}
        id={id}
        type="button"
        className={`dropdown-trigger ${isOpen ? "open" : ""} ${
          !selectedProject ? "placeholder" : ""
        }`}
        onClick={handleToggle}
        aria-haspopup="listbox"
        aria-expanded={isOpen}
        aria-labelledby={`${id}-label`}
      >
        {selectedProject ? (
          <>
            <div className="option-primary">{selectedProject.name}</div>
            <div className="option-secondary">
              {selectedProject.account_name}
            </div>
          </>
        ) : (
          placeholder
        )}
      </button>

      {isOpen && (
        <div
          ref={dropdownRef}
          className="dropdown-options"
          role="listbox"
          aria-labelledby={`${id}-label`}
        >
          {options.map((project) => (
            <button
              key={`${project.account_id}-${project.id}`}
              type="button"
              className={`dropdown-option ${
                project.id === value ? "selected" : ""
              }`}
              onClick={() => handleOptionSelect(project)}
              role="option"
              aria-selected={project.id === value}
            >
              <div className="option-primary">{project.name}</div>
              <div className="option-secondary">{project.account_name}</div>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

export default function App() {
  const oauthResult = useOAuthResult();
  const [responseText, setResponseText] = useState<string | null>(null);
  const [projects, setProjects] = useState<Project[]>([]);
  const [projectsError, setProjectsError] = useState<string | null>(null);
  const [loadingProjects, setLoadingProjects] = useState(false);
  const [selectedProjectId, setSelectedProjectId] = useState<number | null>(
    null
  );
  const [dbtPlatformContext, setDbtPlatformContext] =
    useState<DbtPlatformContext | null>(null);
  const [continuing, setContinuing] = useState(false);
  const [shutdownComplete, setShutdownComplete] = useState(false);

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
          { attempts: 3, delayMs: 400 }
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

  // Fetch saved selected project on load after OAuth success
  useEffect(() => {
    if (oauthResult.status !== "success") return;
    const abortController = new AbortController();
    let cancelled = false;

    (async () => {
      try {
        const res = await fetchWithRetry(
          "/dbt_platform_context",
          { signal: abortController.signal },
          { attempts: 2, delayMs: 400 }
        );
        if (!res.ok || cancelled) return; // if no config yet or server error, skip silently
        const data: DbtPlatformContext = await res.json();
        if (!cancelled) {
          setDbtPlatformContext(data);
        }
      } catch (err) {
        if (isAbortError(err) || cancelled) {
          return;
        }
        // ignore other failures to keep UX consistent
      }
    })();

    return () => {
      cancelled = true;
      abortController.abort();
    };
  }, [oauthResult.status]);

  const onContinue = async () => {
    if (continuing) return;
    setContinuing(true);
    setResponseText(null);
    try {
      const res = await fetchWithRetry(
        "/shutdown",
        { method: "POST" },
        { attempts: 3, delayMs: 400 }
      );
      const text = await res.text();
      if (res.ok) {
        setShutdownComplete(true);
        window.close();
      } else {
        setResponseText(text);
      }
    } catch (err) {
      setResponseText(String(err));
    } finally {
      setContinuing(false);
    }
  };

  const onSelectProject = async (projectIdStr: string) => {
    setDbtPlatformContext(null);
    const projectId = Number(projectIdStr);
    setSelectedProjectId(Number.isNaN(projectId) ? null : projectId);
    const project = projects.find((p) => p.id === projectId);
    if (!project) return;
    try {
      const res = await fetchWithRetry(
        "/selected_project",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            account_id: project.account_id,
            project_id: project.id,
          }),
        },
        { attempts: 3, delayMs: 400 }
      );
      if (res.ok) {
        const data = await res.json();
        setDbtPlatformContext(data);
      } else {
        setResponseText(await res.text());
        setDbtPlatformContext(null);
      }
    } catch (err) {
      setResponseText(String(err));
      setDbtPlatformContext(null);
    }
  };

  return (
    <div className="app-container">
      <div className="logo-container">
        <img src={dbtLogoBLK} alt="dbt" className="logo logo-light" />
        <img src={dbtLogoWHT} alt="dbt" className="logo logo-dark" />
      </div>
      <div className="app-content">
        <header className="app-header">
          <h1>dbt Platform Setup</h1>
          <p>Configure your dbt Platform connection</p>
        </header>

        {oauthResult.status === "error" && (
          <section className="error-section">
            <div className="section-header">
              <h2>Authentication Error</h2>
              <p>There was a problem during authentication</p>
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
              <h2>Select a Project</h2>
              <p>Choose the dbt project you want to work with</p>
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
                  <label
                    htmlFor="project-select"
                    className="form-label"
                    id="project-select-label"
                  >
                    Available Projects
                  </label>
                  <CustomDropdown
                    id="project-select"
                    value={selectedProjectId}
                    onChange={onSelectProject}
                    options={projects}
                    placeholder="Choose a project"
                  />
                </div>
              )}
            </div>
          </section>
        )}

        {dbtPlatformContext && !shutdownComplete && (
          <section className="context-section">
            <div className="section-header">
              <h2>Current Configuration</h2>
              <p>Your dbt Platform context is ready</p>
            </div>

            <div className="context-details">
              <div className="context-item">
                <strong>User ID:</strong>{" "}
                {dbtPlatformContext.decoded_access_token?.decoded_claims.sub}
              </div>

              {dbtPlatformContext.dev_environment && (
                <div className="context-item">
                  <strong>Development Environment:</strong>
                  <div className="environment-details">
                    <span className="env-name">
                      {dbtPlatformContext.dev_environment.name}
                    </span>
                  </div>
                </div>
              )}

              {dbtPlatformContext.prod_environment && (
                <div className="context-item">
                  <strong>Production Environment:</strong>
                  <div className="environment-details">
                    <span className="env-name">
                      {dbtPlatformContext.prod_environment.name}
                    </span>
                  </div>
                </div>
              )}
            </div>
          </section>
        )}

        {dbtPlatformContext && !shutdownComplete && (
          <div className="button-container">
            <button
              onClick={onContinue}
              className="primary-button"
              disabled={selectedProjectId === null || continuing}
            >
              {continuing ? "Closing…" : "Continue"}
            </button>
          </div>
        )}

        {shutdownComplete && (
          <section className="completion-section">
            <div className="completion-card">
              <h2>All Set!</h2>
              <p>
                Your dbt Platform setup has finished. This window can now be
                closed.
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
