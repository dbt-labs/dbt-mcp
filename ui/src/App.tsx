import { useEffect, useMemo, useState } from "react";
import "./App.css";

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
  user_id: number;
};

function parseHash(): URLSearchParams {
  const hash = window.location.hash.startsWith("#")
    ? window.location.hash.slice(1)
    : window.location.hash;
  const query = hash.startsWith("?") ? hash.slice(1) : hash;
  return new URLSearchParams(query);
}

function useOAuthResult(): string | null {
  const params = useMemo(() => parseHash(), []);
  const status = params.get("status");
  return status;
}

export default function App() {
  const oauthResult = useOAuthResult();
  const [responseText, setResponseText] = useState<string | null>(null);
  const [projects, setProjects] = useState<Project[]>([]);
  const [projectsError, setProjectsError] = useState<string | null>(null);
  const [loadingProjects, setLoadingProjects] = useState(false);
  const [selectedProjectId, setSelectedProjectId] = useState<number | "">("");
  const [dbtPlatformContext, setDbtPlatformContext] =
    useState<DbtPlatformContext | null>(null);

  // Load available projects after OAuth success
  useEffect(() => {
    if (oauthResult !== "success") return;
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
  }, [oauthResult]);

  // Fetch saved selected project on load after OAuth success
  useEffect(() => {
    if (oauthResult !== "success") return;
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
  }, [oauthResult]);

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
    setSelectedProjectId(Number.isNaN(projectId) ? "" : projectId);
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
    <div>
      {oauthResult === "success" && (
        <div style={{ marginTop: 16 }}>
          <h3>Select a project</h3>
          {loadingProjects && <div>Loading projects…</div>}
          {projectsError && (
            <div style={{ color: "red" }}>Error: {projectsError}</div>
          )}
          {!loadingProjects && !projectsError && (
            <select
              value={selectedProjectId}
              onChange={(e) => onSelectProject(e.target.value)}
            >
              <option value="">-- choose a project --</option>
              {projects.map((p) => (
                <option key={`${p.account_id}-${p.id}`} value={p.id}>
                  {p.account_name} / {p.name}
                </option>
              ))}
            </select>
          )}
        </div>
      )}
      {dbtPlatformContext && (
        <div style={{ marginTop: 16 }}>
          <h3>Current dbt Platform context</h3>
          <pre>{JSON.stringify(dbtPlatformContext, null, 2)}</pre>
        </div>
      )}
      {dbtPlatformContext && (
        <button onClick={onContinue} style={{ marginLeft: 8 }}>
          Continue
        </button>
      )}
      {responseText && <pre>{responseText}</pre>}
    </div>
  );
}
