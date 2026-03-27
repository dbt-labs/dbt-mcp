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

type Environment = {
  id: number;
  name: string;
  deployment_type: string | null;
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

type CustomDropdownProps<T> = {
  value: number | null;
  onChange: (value: string) => void;
  options: T[];
  placeholder: string;
  id: string;
  getOptionId: (option: T) => number;
  getPrimary: (option: T) => string;
  getSecondary: (option: T) => string;
  getSearchText: (option: T) => string;
};

function CustomDropdown<T,>({
  value,
  onChange,
  options,
  placeholder,
  id,
  getOptionId,
  getPrimary,
  getSecondary,
  getSearchText,
}: CustomDropdownProps<T>) {
  const [isOpen, setIsOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const dropdownRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const searchRef = useRef<HTMLInputElement>(null);

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

  // Handle keyboard navigation on the trigger button (open/close/escape)
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
        setSearchQuery("");
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

  // Auto-focus search input when dropdown opens
  useEffect(() => {
    if (isOpen) searchRef.current?.focus();
  }, [isOpen]);

  const query = searchQuery.trim().toLowerCase();
  const filteredOptions = query
    ? options.filter((o) => getSearchText(o).toLowerCase().includes(query))
    : options;

  const selectedOption = options.find((o) => getOptionId(o) === value);

  const handleToggle = () => {
    setIsOpen(!isOpen);
  };

  const handleOptionSelect = (option: T) => {
    onChange(getOptionId(option).toString());
    setIsOpen(false);
    setSearchQuery("");
    triggerRef.current?.focus();
  };

  return (
    <div className="custom-dropdown" ref={dropdownRef}>
      <button
        ref={triggerRef}
        id={id}
        type="button"
        className={`dropdown-trigger ${isOpen ? "open" : ""} ${
          !selectedOption ? "placeholder" : ""
        }`}
        onClick={handleToggle}
        aria-haspopup="listbox"
        aria-expanded={isOpen}
        aria-labelledby={`${id}-label`}
      >
        {selectedOption ? (
          <>
            <div className="option-primary">{getPrimary(selectedOption)}</div>
            <div className="option-secondary">{getSecondary(selectedOption)}</div>
          </>
        ) : (
          placeholder
        )}
      </button>

      {isOpen && (
        <div
          className="dropdown-options"
          role="listbox"
          aria-labelledby={`${id}-label`}
        >
          <div className="dropdown-search-wrapper">
            <input
              ref={searchRef}
              type="text"
              className="dropdown-search"
              placeholder="Search..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              onClick={(e) => e.stopPropagation()}
              aria-label="Search"
            />
          </div>
          {filteredOptions.map((option) => (
            <button
              key={getOptionId(option)}
              type="button"
              className={`dropdown-option ${
                getOptionId(option) === value ? "selected" : ""
              }`}
              onClick={() => handleOptionSelect(option)}
              role="option"
              aria-selected={getOptionId(option) === value}
            >
              <div className="option-primary">{getPrimary(option)}</div>
              <div className="option-secondary">{getSecondary(option)}</div>
            </button>
          ))}
          {filteredOptions.length === 0 && (
            <div className="dropdown-no-results">No results found</div>
          )}
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
  const [environments, setEnvironments] = useState<Environment[]>([]);
  const [loadingEnvironments, setLoadingEnvironments] = useState(false);
  const [environmentsError, setEnvironmentsError] = useState<string | null>(
    null
  );
  const [selectedEnvironmentId, setSelectedEnvironmentId] = useState<
    number | null
  >(null);

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
      if (isNetworkError(err)) {
        setResponseText(
          "Something went wrong when setting up the authentication. Please close this window and try again."
        );
      } else {
        setResponseText(String(err));
      }
    } finally {
      setContinuing(false);
    }
  };

  const onSelectProject = async (projectIdStr: string) => {
    setDbtPlatformContext(null);
    setEnvironments([]);
    setSelectedEnvironmentId(null);
    setEnvironmentsError(null);
    const projectId = Number(projectIdStr);
    setSelectedProjectId(Number.isNaN(projectId) ? null : projectId);
    const project = projects.find((p) => p.id === projectId);
    if (!project) return;

    // Fetch environments for the selected project
    setLoadingEnvironments(true);
    try {
      const res = await fetchWithRetry(
        "/environments",
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
        const data: Environment[] = await res.json();
        setEnvironments(data);
        // Pre-select production environment if available
        const prodEnv = data.find(
          (env) =>
            env.deployment_type &&
            env.deployment_type.toLowerCase() === "production"
        );
        if (prodEnv) {
          setSelectedEnvironmentId(prodEnv.id);
        } else if (data.length > 0) {
          setSelectedEnvironmentId(data[0].id);
        }
      } else {
        setEnvironmentsError(await res.text());
      }
    } catch (err) {
      setEnvironmentsError(String(err));
    } finally {
      setLoadingEnvironments(false);
    }
  };

  const onSelectEnvironment = (envIdStr: string) => {
    const envId = Number(envIdStr);
    setSelectedEnvironmentId(Number.isNaN(envId) ? null : envId);
  };

  const onConfirmSelection = async () => {
    const project = projects.find((p) => p.id === selectedProjectId);
    if (!project || selectedEnvironmentId === null) return;

    try {
      const res = await fetchWithRetry(
        "/selected_project",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            account_id: project.account_id,
            project_id: project.id,
            prod_environment_id: selectedEnvironmentId,
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
              <p>
                The dbt MCP server could not authenticate with dbt Platform
              </p>
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
                    getOptionId={(p) => p.id}
                    getPrimary={(p) => p.name}
                    getSecondary={(p) => p.account_name}
                    getSearchText={(p) => `${p.name} ${p.account_name}`}
                  />
                </div>
              )}

              {loadingEnvironments && (
                <div className="loading-state">
                  <div className="spinner"></div>
                  <span>Loading environments…</span>
                </div>
              )}

              {environmentsError && (
                <div className="error-state">
                  <strong>Error loading environments</strong>
                  <p>{environmentsError}</p>
                </div>
              )}

              {!loadingEnvironments &&
                !environmentsError &&
                selectedProjectId !== null &&
                environments.length > 0 && (
                  <div className="form-group">
                    <label
                      htmlFor="environment-select"
                      className="form-label"
                      id="environment-select-label"
                    >
                      Deployment Environment
                    </label>
                    <CustomDropdown
                      id="environment-select"
                      value={selectedEnvironmentId}
                      onChange={onSelectEnvironment}
                      options={environments}
                      placeholder="Choose an environment"
                      getOptionId={(e) => e.id}
                      getPrimary={(e) => e.name}
                      getSecondary={(e) =>
                        e.deployment_type
                          ? `${e.deployment_type} · ${e.id}`
                          : String(e.id)
                      }
                      getSearchText={(e) =>
                        `${e.name} ${e.deployment_type ?? ""}`
                      }
                    />
                  </div>
                )}

              {!loadingEnvironments &&
                !environmentsError &&
                selectedProjectId !== null &&
                environments.length === 0 && (
                  <div className="error-state">
                    <strong>No environments available</strong>
                    <p>
                      This project has no non-development environments. Please
                      configure an environment in dbt Cloud first.
                    </p>
                  </div>
                )}

              {selectedProjectId !== null &&
                environments.length > 0 &&
                selectedEnvironmentId !== null &&
                !dbtPlatformContext && (
                  <div className="button-container" style={{ marginTop: "1rem" }}>
                    <button
                      onClick={onConfirmSelection}
                      className="primary-button"
                    >
                      Confirm Selection
                    </button>
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
                  <strong>Deployment Environment:</strong>
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
                The dbt MCP server is authenticated and configured with your
                dbt Platform account. This window can now be closed.
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
