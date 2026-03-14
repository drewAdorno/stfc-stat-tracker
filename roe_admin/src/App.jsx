import { useEffect, useMemo, useState } from "react";

const PASSWORD_KEY = "ncc_admin_password";
const AUTH_KEY = "ncc_admin_auth";

const initialForm = {
  offender_query: "",
  violation_type: "OPC hit",
  victim_name: "",
  offense_date: new Date().toISOString().slice(0, 10),
  screenshots: "",
  notes: "",
  offender_overrides: {
    alliance_id: "",
    alliance_tag: "",
    alliance_name: "",
  },
};

const violationOptions = [
  "OPC hit",
  "Token space hit",
  "Armada interference",
  "Friendly alliance hit",
];

function formatDate(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleDateString("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

function classNames(...values) {
  return values.filter(Boolean).join(" ");
}

function parseScreenshotItems(value) {
  return String(value || "")
    .split(/\r?\n|,/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function screenshotUrl(value) {
  if (!value) return "";
  if (/^https?:\/\//i.test(value)) return value;
  return value.startsWith("/") ? value : `/${value}`;
}

async function apiFetch(path, password, options = {}) {
  const headers = {
    "X-Admin-Password": password,
    ...(options.headers || {}),
  };
  if (!(options.body instanceof FormData) && !headers["Content-Type"]) {
    headers["Content-Type"] = "application/json";
  }

  const response = await fetch(path, {
    ...options,
    headers,
  });

  if (response.status === 401) {
    throw new Error("auth");
  }

  const contentType = response.headers.get("content-type") || "";
  const payload = contentType.includes("application/json")
    ? await response.json()
    : await response.text();

  if (!response.ok) {
    const message = typeof payload === "object" ? payload.detail : payload;
    throw new Error(message || "Request failed");
  }

  return payload;
}

async function uploadScreenshots(files, password) {
  if (!files.length) return [];
  const body = new FormData();
  files.forEach((file) => body.append("files", file));
  const payload = await apiFetch("/api/roe/uploads", password, {
    method: "POST",
    body,
  });
  return payload.screenshots || [];
}

function StatCard({ label, value, subtext }) {
  return (
    <div className="stat-card">
      <div className="stat-label">{label}</div>
      <div className="stat-value">{value}</div>
      <div className="stat-subtext">{subtext}</div>
    </div>
  );
}

function TalliesList({ title, rows, type }) {
  return (
    <section className="panel">
      <div className="panel-title-row">
        <h3>{title}</h3>
      </div>
      {!rows.length ? (
        <p className="empty-state">Nothing logged yet.</p>
      ) : (
        <div className="tallies-list">
          {rows.map((row, index) => (
            <div className="tally-row" key={`${type}-${index}-${row.offense_count}`}>
              <div className="tally-rank">{String(index + 1).padStart(2, "0")}</div>
              <div className="tally-main">
                <div className="tally-name">
                  {type === "player"
                    ? row.offender_name
                    : row.offender_alliance_tag || row.offender_alliance_name || "Unknown alliance"}
                </div>
                <div className="tally-meta">
                  {type === "player"
                    ? row.offender_alliance_tag || row.offender_alliance_name || "Alliance unknown"
                    : `${row.unique_offender_count} unique offenders`}
                </div>
              </div>
              <div className="tally-count">{row.offense_count}</div>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}

function SiteNav() {
  return (
    <nav className="site-nav" aria-label="Primary">
      <a href="/index">Tracker</a>
      <a href="/calendar">Calendar</a>
      <a className="nav-admin" href="/server">
        Server <span className="nav-lock" aria-hidden="true">&#128274;</span>
      </a>
      <a className="nav-admin" href="/leaderboard">
        Leaderboard <span className="nav-lock" aria-hidden="true">&#128274;</span>
      </a>
      <a className="active nav-admin" href="/roe-admin/">
        ROE Violations <span className="nav-lock" aria-hidden="true">&#128274;</span>
      </a>
      <a className="nav-admin" href="/admin">
        Admin <span className="nav-lock" aria-hidden="true">&#128274;</span>
      </a>
    </nav>
  );
}

function App() {
  const [password, setPassword] = useState(() => sessionStorage.getItem(PASSWORD_KEY) || "");
  const [passwordInput, setPasswordInput] = useState(password);
  const [summary, setSummary] = useState(null);
  const [violations, setViolations] = useState([]);
  const [loading, setLoading] = useState(false);
  const [bootError, setBootError] = useState("");
  const [form, setForm] = useState(initialForm);
  const [submitting, setSubmitting] = useState(false);
  const [submitMessage, setSubmitMessage] = useState("");
  const [playerResults, setPlayerResults] = useState([]);
  const [searchLoading, setSearchLoading] = useState(false);
  const [filterText, setFilterText] = useState("");
  const [screenshotFiles, setScreenshotFiles] = useState([]);
  const [screenshotInputKey, setScreenshotInputKey] = useState(0);

  const screenshotPreviews = useMemo(
    () =>
      screenshotFiles.map((file) => ({
        id: `${file.name}-${file.size}-${file.lastModified}`,
        name: file.name,
        url: URL.createObjectURL(file),
      })),
    [screenshotFiles],
  );

  useEffect(
    () => () => {
      screenshotPreviews.forEach((preview) => URL.revokeObjectURL(preview.url));
    },
    [screenshotPreviews],
  );

  async function loadDashboard(activePassword) {
    setLoading(true);
    setBootError("");
    try {
      const [summaryPayload, violationsPayload] = await Promise.all([
        apiFetch("/api/roe/summary", activePassword),
        apiFetch("/api/roe/violations?limit=200", activePassword),
      ]);
      sessionStorage.setItem(PASSWORD_KEY, activePassword);
      sessionStorage.setItem(AUTH_KEY, "ok");
      setPassword(activePassword);
      setSummary(summaryPayload);
      setViolations(violationsPayload.violations || []);
    } catch (error) {
      if (error.message === "auth") {
        sessionStorage.removeItem(PASSWORD_KEY);
        sessionStorage.removeItem(AUTH_KEY);
        setPassword("");
        setBootError("Password rejected. Please try again.");
      } else {
        setBootError(error.message || "Failed to load ROE data.");
      }
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    if (password) {
      loadDashboard(password);
    }
  }, []);

  useEffect(() => {
    const query = form.offender_query.trim();
    if (!password || query.length < 2) {
      setPlayerResults([]);
      return undefined;
    }

    const controller = new AbortController();
    const timer = setTimeout(async () => {
      setSearchLoading(true);
      try {
        const payload = await fetch(`/api/players/search?q=${encodeURIComponent(query)}&limit=8`, {
          headers: {
            "X-Admin-Password": password,
          },
          signal: controller.signal,
        }).then(async (response) => {
          if (response.status === 401) {
            throw new Error("auth");
          }
          const data = await response.json();
          if (!response.ok) {
            throw new Error(data.detail || "Search failed");
          }
          return data;
        });
        setPlayerResults(payload.players || []);
      } catch (error) {
        if (error.name !== "AbortError") {
          if (error.message === "auth") {
            setBootError("Admin password expired. Sign in again.");
            sessionStorage.removeItem(PASSWORD_KEY);
            setPassword("");
          }
          setPlayerResults([]);
        }
      } finally {
        setSearchLoading(false);
      }
    }, 250);

    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [form.offender_query, password]);

  useEffect(() => {
    const query = form.offender_query.trim().toLowerCase();
    if (!query || !playerResults.length) return;
    const exactMatch = playerResults.find(
      (player) => player.name.trim().toLowerCase() === query || player.player_id === form.offender_query.trim(),
    );
    if (!exactMatch) return;
    setForm((current) => ({
      ...current,
      offender_overrides: {
        ...current.offender_overrides,
        alliance_id: exactMatch.alliance_id || "",
        alliance_tag: exactMatch.alliance_tag || "",
        alliance_name: exactMatch.alliance_name || "",
      },
    }));
  }, [form.offender_query, playerResults]);

  const filteredViolations = useMemo(() => {
    const query = filterText.trim().toLowerCase();
    if (!query) return violations;
    return violations.filter((entry) =>
      [
        entry.offender_name,
        entry.offender_alliance_tag,
        entry.offender_alliance_name,
        entry.violation_type,
        entry.victim_name,
        entry.screenshots,
        entry.notes,
        entry.reported_by,
      ]
        .join(" ")
        .toLowerCase()
        .includes(query),
    );
  }, [violations, filterText]);

  function updateForm(field, value) {
    setForm((current) => ({
      ...current,
      [field]: value,
    }));
  }

  function updateOverride(field, value) {
    setForm((current) => ({
      ...current,
      offender_overrides: { ...current.offender_overrides, [field]: value },
    }));
  }

  function choosePlayer(player) {
    setForm((current) => ({
      ...current,
      offender_query: player.name,
      offender_overrides: {
        ...current.offender_overrides,
        alliance_id: player.alliance_id || "",
        alliance_tag: player.alliance_tag || "",
        alliance_name: player.alliance_name || "",
      },
    }));
    setPlayerResults([]);
  }

  async function submitForm(event) {
    event.preventDefault();
    setSubmitting(true);
    setSubmitMessage("");
    try {
      const uploadedScreenshots = await uploadScreenshots(screenshotFiles, password);
      const payload = await apiFetch("/api/roe/violations", password, {
        method: "POST",
        body: JSON.stringify({
          ...form,
          reported_by: form.victim_name,
          screenshots: uploadedScreenshots.join("\n"),
        }),
      });

      setSubmitMessage(`Logged violation #${payload.violation_id} for ${payload.identity.name}.`);
      setForm(initialForm);
      setScreenshotFiles([]);
      setScreenshotInputKey((current) => current + 1);
      setSummary(payload.payload);
      await loadDashboard(password);
    } catch (error) {
      if (error.message === "auth") {
        setBootError("Admin password expired. Sign in again.");
        sessionStorage.removeItem(PASSWORD_KEY);
        sessionStorage.removeItem(AUTH_KEY);
        setPassword("");
      } else {
        setSubmitMessage(error.message || "Failed to save violation.");
      }
    } finally {
      setSubmitting(false);
    }
  }

  function signOut() {
    sessionStorage.removeItem(PASSWORD_KEY);
    sessionStorage.removeItem(AUTH_KEY);
    setPassword("");
    setPasswordInput("");
    setSummary(null);
    setViolations([]);
    setPlayerResults([]);
  }

  async function handleLogin(event) {
    event.preventDefault();
    await loadDashboard(passwordInput);
  }

  if (!password || !summary) {
    return (
      <div className="screen login-screen">
        <div className="noise" />
        <div className="login-shell">
          <div className="login-layout">
            <SiteNav />
            <div className="login-intro">
              <div className="eyebrow">NCC Alliance Control</div>
              <h1>ROE Violations</h1>
              <p className="lead">
                Manual incident entry, offender tallies, and alliance history in one place.
              </p>
            </div>
          <form className="login-card" onSubmit={handleLogin}>
            <label htmlFor="admin-password">Admin password</label>
            <input
              id="admin-password"
              type="password"
              value={passwordInput}
              onChange={(event) => setPasswordInput(event.target.value)}
              placeholder="Enter admin password"
              autoComplete="current-password"
            />
            <button type="submit" disabled={!passwordInput || loading}>
              {loading ? "Checking..." : "Open page"}
            </button>
            {bootError ? <div className="form-status error">{bootError}</div> : null}
          </form>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="screen">
      <div className="noise" />
      <div className="app-shell">
        <SiteNav />
        <div className="page-actions">
          <div className="eyebrow">NCC ROE Violations</div>
          <div className="page-action-buttons">
            <button className="ghost-button" type="button" onClick={() => loadDashboard(password)}>
              Refresh
            </button>
            <button className="ghost-button" type="button" onClick={signOut}>
              Sign out
            </button>
          </div>
        </div>

        <section className="stats-grid">
          <StatCard
            label="Total Violations"
            value={summary.violation_count}
            subtext="All logged incidents in SQLite"
          />
          <StatCard
            label="Known Offenders"
            value={summary.unique_offender_count}
            subtext="Unique players with at least one breach"
          />
          <StatCard
            label="Alliances Tracked"
            value={summary.alliance_count}
            subtext="Distinct alliances represented in the log"
          />
          <StatCard
            label="Last Updated"
            value={formatDate(summary.updated_at)}
            subtext="Export refreshed automatically after each write"
          />
        </section>

        <div className="main-grid">
          <section className="panel form-panel">
            <div className="panel-title-row">
              <div>
                <h2>Log New Violation</h2>
                  <p>Type the offender name freely, upload the proof images, and save the incident.</p>
              </div>
            </div>

            <form className="incident-form" onSubmit={submitForm}>
              <div className="field">
                <label htmlFor="offender_query">Offender</label>
                <input
                  id="offender_query"
                  value={form.offender_query}
                  onChange={(event) => updateForm("offender_query", event.target.value)}
                  placeholder="Player name or player id"
                  required
                />
                {searchLoading ? <div className="hint">Searching players...</div> : null}
                {playerResults.length ? (
                  <div className="search-results">
                    {playerResults.map((player) => (
                      <button
                        key={`${player.player_id}-${player.name}`}
                        className="search-result"
                        type="button"
                        onClick={() => choosePlayer(player)}
                      >
                        <span>{player.name}</span>
                        <span className="search-meta">
                          {player.alliance_tag || player.alliance_name || "No alliance"}
                        </span>
                      </button>
                    ))}
                  </div>
                ) : null}
                <div className="hint">Alliance details below auto-fill when this matches a known player.</div>
              </div>

              <div className="field-row">
                <div className="field">
                  <label htmlFor="violation_type">Violation type</label>
                  <select
                    id="violation_type"
                    value={form.violation_type}
                    onChange={(event) => updateForm("violation_type", event.target.value)}
                  >
                    {violationOptions.map((option) => (
                      <option key={option} value={option}>
                        {option}
                      </option>
                    ))}
                  </select>
                </div>

                <div className="field">
                  <label htmlFor="offense_date">Offense date</label>
                  <input
                    id="offense_date"
                    type="date"
                    value={form.offense_date}
                    onChange={(event) => updateForm("offense_date", event.target.value)}
                  />
                </div>
              </div>

              <div className="field">
                <label htmlFor="victim_name">Victim</label>
                <input
                  id="victim_name"
                  value={form.victim_name}
                  onChange={(event) => updateForm("victim_name", event.target.value)}
                  placeholder="Alliance member hit"
                />
              </div>

              <div className="override-grid">
                <div className="field">
                  <label htmlFor="override_tag">Alliance tag</label>
                  <input
                    id="override_tag"
                    value={form.offender_overrides.alliance_tag}
                    onChange={(event) => updateOverride("alliance_tag", event.target.value)}
                    placeholder="Auto-filled when available"
                  />
                </div>
                <div className="field">
                  <label htmlFor="override_name_alliance">Alliance name</label>
                  <input
                    id="override_name_alliance"
                    value={form.offender_overrides.alliance_name}
                    onChange={(event) => updateOverride("alliance_name", event.target.value)}
                    placeholder="Auto-filled when available"
                  />
                </div>
                <div className="field">
                  <label htmlFor="override_id">Alliance id</label>
                  <input
                    id="override_id"
                    value={form.offender_overrides.alliance_id}
                    onChange={(event) => updateOverride("alliance_id", event.target.value)}
                    placeholder="Optional manual override"
                  />
                </div>
              </div>

              <div className="field">
                <label htmlFor="screenshots">Screenshots</label>
                <input
                  key={screenshotInputKey}
                  id="screenshots"
                  type="file"
                  accept="image/*"
                  multiple
                  onChange={(event) => setScreenshotFiles(Array.from(event.target.files || []))}
                />
                <div className="hint">Upload one or more screenshots. They will be stored with the violation automatically.</div>
                {screenshotPreviews.length ? (
                  <div className="screenshot-preview-grid">
                    {screenshotPreviews.map((preview) => (
                      <figure className="screenshot-preview-card" key={preview.id}>
                        <img src={preview.url} alt={preview.name} />
                        <figcaption>{preview.name}</figcaption>
                      </figure>
                    ))}
                  </div>
                ) : null}
              </div>

              <div className="field">
                <label htmlFor="notes">Notes</label>
                <textarea
                  id="notes"
                  value={form.notes}
                  onChange={(event) => updateForm("notes", event.target.value)}
                  placeholder="What happened, what was hit, follow-up, or context"
                  rows={4}
                />
              </div>

              <div className="form-footer">
                <button className="primary-button" type="submit" disabled={submitting}>
                  {submitting ? "Saving..." : "Record violation"}
                </button>
                {submitMessage ? (
                  <div
                    className={classNames(
                      "form-status",
                      submitMessage.toLowerCase().includes("failed") ? "error" : "success",
                    )}
                  >
                    {submitMessage}
                  </div>
                ) : null}
              </div>
            </form>
          </section>

          <div className="side-stack">
            <TalliesList
              title="Top Individual Offenders"
              rows={(summary.player_tallies || []).slice(0, 8)}
              type="player"
            />
            <TalliesList
              title="Top Offending Alliances"
              rows={(summary.alliance_tallies || []).slice(0, 8)}
              type="alliance"
            />
          </div>
        </div>

        <section className="panel">
          <div className="panel-title-row">
            <div>
              <h2>Recent Violations</h2>
                  <p>Filter locally across offender, alliance, victim, screenshots, notes, and reporter.</p>
            </div>
            <input
              className="filter-input"
              value={filterText}
              onChange={(event) => setFilterText(event.target.value)}
              placeholder="Filter incidents..."
            />
          </div>

          {!filteredViolations.length ? (
            <p className="empty-state">No violations match this filter.</p>
          ) : (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Date</th>
                    <th>Offender</th>
                    <th>Alliance</th>
                    <th>Violation</th>
                    <th>Victim</th>
                    <th>Reported by</th>
                    <th>Screenshots</th>
                    <th>Notes</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredViolations.map((entry) => (
                    <tr key={entry.id}>
                      <td>{formatDate(entry.offense_date)}</td>
                      <td>{entry.offender_name}</td>
                      <td>{entry.offender_alliance_tag || entry.offender_alliance_name || "-"}</td>
                      <td>{entry.violation_type}</td>
                      <td>{entry.victim_name || "-"}</td>
                      <td>{entry.reported_by || "-"}</td>
                      <td className="screenshot-cell">
                        {parseScreenshotItems(entry.screenshots).length ? (
                          parseScreenshotItems(entry.screenshots).map((item) => (
                            <a
                              key={item}
                              className="screenshot-thumb"
                              href={screenshotUrl(item)}
                              target="_blank"
                              rel="noreferrer"
                            >
                              <img src={screenshotUrl(item)} alt="Violation screenshot" loading="lazy" />
                            </a>
                          ))
                        ) : (
                          "-"
                        )}
                      </td>
                      <td className="notes-cell">{entry.notes || "-"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>
      </div>
    </div>
  );
}

export default App;
