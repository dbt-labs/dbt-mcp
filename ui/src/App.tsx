import { useEffect, useMemo, useState, useRef } from "react";
import "./App.css";

// Import SVG files as URLs for theme switching
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

  // Load available projects after OAuth success
  useEffect(() => {
    if (oauthResult.status !== "success") return;
    setLoadingProjects(true);
    setProjectsError(null);
    fetch("/projects")
      .then(async (r) => {
        if (!r.ok) throw new Error(`Failed to load projects (${r.status})`);
        return r.json();
      })
      .then((data: Project[]) => {
        setProjects(data);
      })
      .catch((err: unknown) => {
        const msg = err instanceof Error ? err.message : String(err);
        setProjectsError(msg);
      })
      .finally(() => setLoadingProjects(false));
  }, [oauthResult.status]);

  // Fetch saved selected project on load after OAuth success
  useEffect(() => {
    if (oauthResult.status !== "success") return;
    (async () => {
      try {
        const res = await fetch("/dbt_platform_context");
        if (!res.ok) return; // if no config yet or server error, skip silently
        const data: DbtPlatformContext = await res.json();
        setDbtPlatformContext(data);
      } catch {
        // ignore
      }
    })();
  }, [oauthResult.status]);

  const onContinue = async () => {
    try {
      const res = await fetch("/shutdown", { method: "POST" });
      const text = await res.text();
      if (res.ok) {
        window.close();
      } else {
        setResponseText(text);
      }
    } catch (err) {
      setResponseText(String(err));
    }
  };

  const onSelectProject = async (projectIdStr: string) => {
    setDbtPlatformContext(null);
    const projectId = Number(projectIdStr);
    setSelectedProjectId(Number.isNaN(projectId) ? null : projectId);
    const project = projects.find((p) => p.id === projectId);
    if (!project) return;
    try {
      const res = await fetch("/selected_project", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          account_id: project.account_id,
          project_id: project.id,
        }),
      });
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

        {oauthResult.status === "success" && (
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

        {dbtPlatformContext && (
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

        {dbtPlatformContext && (
          <div className="button-container">
            <button
              onClick={onContinue}
              className="primary-button"
              disabled={selectedProjectId === null}
            >
              Continue
            </button>
          </div>
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
