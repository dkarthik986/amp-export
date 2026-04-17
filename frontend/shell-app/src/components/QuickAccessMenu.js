import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useAuth } from "../AuthContext";

const API_BASE = process.env.REACT_APP_API_BASE_URL;
const EXPORT_JOBS_RECENT_URL = `${API_BASE}/api/export-jobs/recent?limit=12`;
const EXPORT_JOB_REFRESH_EVENT = "swift-export-job-created";

function extractFilenameFromDisposition(disposition) {
  if (!disposition) return null;
  const utfMatch = disposition.match(/filename\*=UTF-8''([^;]+)/i);
  if (utfMatch?.[1]) return decodeURIComponent(utfMatch[1]);
  const plainMatch = disposition.match(/filename="?([^"]+)"?/i);
  return plainMatch?.[1] || null;
}

function triggerDownload(blob, fileName) {
  const url = window.URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = fileName;
  document.body.appendChild(link);
  link.click();
  link.remove();
  window.URL.revokeObjectURL(url);
}

function formatRelativeTime(value) {
  if (!value) return "Just now";
  const when = new Date(value).getTime();
  if (Number.isNaN(when)) return "Just now";
  const diffMs = Math.max(0, Date.now() - when);
  const diffMinutes = Math.floor(diffMs / 60000);
  if (diffMinutes < 1) return "Just now";
  if (diffMinutes < 60) return `${diffMinutes} min ago`;
  const diffHours = Math.floor(diffMinutes / 60);
  if (diffHours < 24) return `${diffHours} hr ago`;
  const diffDays = Math.floor(diffHours / 24);
  return `${diffDays} day${diffDays === 1 ? "" : "s"} ago`;
}

function formatSectionLabel(key) {
  const known = {
    table: "Result Table",
    header: "Header",
    rawpayload: "Raw Payload",
    payload: "Extended text",
    history: "History",
    details: "All Fields",
  };
  return known[key] || String(key || "")
    .replace(/([a-z])([A-Z])/g, "$1 $2")
    .replace(/[._-]+/g, " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function getExpiry(token) {
  try {
    const payload = JSON.parse(atob(token.split(".")[1].replace(/-/g, "+").replace(/_/g, "/")));
    return payload?.exp ? payload.exp * 1000 : null;
  } catch {
    return null;
  }
}

function useSessionCountdown() {
  const [msLeft, setMsLeft] = useState(null);

  useEffect(() => {
    const token = localStorage.getItem("token");
    if (!token) return undefined;

    const expiry = getExpiry(token);
    if (!expiry) return undefined;

    const tick = () => setMsLeft(Math.max(0, expiry - Date.now()));
    tick();

    const intervalId = setInterval(tick, 1000);
    return () => clearInterval(intervalId);
  }, []);

  return msLeft;
}

function getSessionState(msLeft) {
  if (msLeft === null) {
    return {
      tone: "neutral",
      label: "Session unavailable",
      detail: "No active token expiry was found.",
      accentColor: "#94a3b8",
      backgroundColor: "#f8fafc",
    };
  }

  const totalSeconds = Math.floor(msLeft / 1000);
  const hh = Math.floor(totalSeconds / 3600);
  const mm = Math.floor((totalSeconds % 3600) / 60);
  const ss = totalSeconds % 60;
  const pad = (value) => String(value).padStart(2, "0");
  const countdown = hh > 0 ? `${pad(hh)}:${pad(mm)}:${pad(ss)}` : `${pad(mm)}:${pad(ss)}`;

  if (msLeft === 0) {
    return {
      tone: "danger",
      label: "Session expired",
      detail: "Your current login session has expired.",
      value: "Expired",
      accentColor: "#ef4444",
      backgroundColor: "#fef2f2",
    };
  }

  if (msLeft < 60000) {
    return {
      tone: "danger",
      label: "Session expires soon",
      detail: "Less than 1 minute remaining.",
      value: countdown,
      accentColor: "#ef4444",
      backgroundColor: "#fef2f2",
    };
  }

  if (msLeft < 300000) {
    return {
      tone: "warning",
      label: "Session expires soon",
      detail: "Less than 5 minutes remaining.",
      value: countdown,
      accentColor: "#f97316",
      backgroundColor: "#fff7ed",
    };
  }

  if (msLeft < 900000) {
    return {
      tone: "caution",
      label: "Session active",
      detail: "Less than 15 minutes remaining.",
      value: countdown,
      accentColor: "#eab308",
      backgroundColor: "#fefce8",
    };
  }

  return {
    tone: "success",
    label: "Session active",
    detail: "You still have plenty of time left.",
    value: countdown,
    accentColor: "#22c55e",
    backgroundColor: "#f0fdf4",
  };
}

function buildNotificationText(job) {
  const format = (job.requestedFormat || "").toUpperCase();
  const sections = (job.selectedSections || []).map(formatSectionLabel).join(" + ") || "Result Table";
  const progress = Math.max(0, Number(job.progressPercentage || 0));
  if (job.status === "COMPLETED") return `Export completed: ${sections} (${format})`;
  if (job.status === "CANCELLED") return `Export cancelled: ${sections} (${format})`;
  if (job.status === "FAILED") return `Export failed: ${sections} (${format})`;
  if (job.status === "PROCESSING") return `Export in progress (${progress}%): ${sections} (${format})`;
  return `Export started (${progress}%): ${sections} (${format})`;
}

export default function QuickAccessMenu({ onOpenTab }) {
  const { user, logout, token } = useAuth();
  const [open, setOpen] = useState(false);
  const [notificationsOpen, setNotificationsOpen] = useState(false);
  const [activeSection, setActiveSection] = useState("session");
  const [jobs, setJobs] = useState([]);
  const [unreadIds, setUnreadIds] = useState([]);
  const [pollIntervalMs, setPollIntervalMs] = useState(8000);
  const menuRef = useRef(null);
  const previousJobsRef = useRef(new Map());
  const pollingRef = useRef(null);
  const msLeft = useSessionCountdown();
  const sessionState = getSessionState(msLeft);

  const authHeaders = useCallback(() => ({
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
  }), [token]);

  const refreshJobs = useCallback(async (markNew = true) => {
    if (!token) {
      setJobs([]);
      setUnreadIds([]);
      return;
    }
    const res = await fetch(EXPORT_JOBS_RECENT_URL, { headers: authHeaders() });
    if (!res.ok) return;
    const payload = await res.json();
    const nextJobs = Array.isArray(payload?.jobs) ? payload.jobs : [];
    if (typeof payload?.pollIntervalMs === "number" && payload.pollIntervalMs > 1000) {
      setPollIntervalMs(payload.pollIntervalMs);
    }

    if (markNew) {
      const previous = previousJobsRef.current;
      const nextUnread = new Set(unreadIds);
      nextJobs.forEach((job) => {
        const before = previous.get(job.id);
        if (!before || before.status !== job.status) {
          nextUnread.add(job.id);
        }
      });
      setUnreadIds([...nextUnread]);
    }

    previousJobsRef.current = new Map(nextJobs.map((job) => [job.id, job]));
    setJobs(nextJobs);
  }, [authHeaders, token, unreadIds]);

  useEffect(() => {
    const handlePointerDown = (event) => {
      if (menuRef.current && !menuRef.current.contains(event.target)) {
        setOpen(false);
        setNotificationsOpen(false);
      }
    };

    document.addEventListener("mousedown", handlePointerDown);
    return () => document.removeEventListener("mousedown", handlePointerDown);
  }, []);

  useEffect(() => {
    if (!token) return undefined;
    refreshJobs(false);

    const startPolling = () => {
      if (pollingRef.current) {
        clearInterval(pollingRef.current);
      }
      pollingRef.current = setInterval(() => {
        refreshJobs(true);
      }, pollIntervalMs);
    };

    startPolling();
    return () => {
      if (pollingRef.current) {
        clearInterval(pollingRef.current);
      }
    };
  }, [pollIntervalMs, refreshJobs, token]);

  useEffect(() => {
    const handleExternalRefresh = (event) => {
      const job = event?.detail;
      if (job?.id) {
        previousJobsRef.current.set(job.id, job);
        setJobs((prev) => [job, ...prev.filter((item) => item.id !== job.id)]);
        setUnreadIds((prev) => Array.from(new Set([job.id, ...prev])));
      }
      refreshJobs(true);
    };

    window.addEventListener(EXPORT_JOB_REFRESH_EVENT, handleExternalRefresh);
    return () => window.removeEventListener(EXPORT_JOB_REFRESH_EVENT, handleExternalRefresh);
  }, [refreshJobs]);

  const initials = user?.name?.split(" ").map((part) => part[0]).join("").slice(0, 2).toUpperCase() || "?";
  const notificationCount = unreadIds.length;

  const notifications = useMemo(() => jobs.map((job) => ({
    ...job,
    title: buildNotificationText(job),
    time: formatRelativeTime(job.updatedAt || job.completedAt || job.createdAt),
    detail: job.status === "PROCESSING" || job.status === "QUEUED"
      ? `${(job.progressPercentage || 0).toLocaleString()}% complete • ${(job.processedCount || 0).toLocaleString()} / ${(job.totalCount || 0).toLocaleString()} messages`
      : `${(job.totalCount || 0).toLocaleString()} messages • ${(job.selectedSections || []).map(formatSectionLabel).join(" + ") || "Result Table"}`,
  })), [jobs]);
  const downloadJob = useCallback(async (job) => {
    if (!job?.id) return;
    const res = await fetch(`${API_BASE}/api/export-jobs/${encodeURIComponent(job.id)}/download`, {
      headers: authHeaders(),
    });
    if (!res.ok) return;
    const blob = await res.blob();
    const fileName = extractFilenameFromDisposition(res.headers.get("Content-Disposition")) || job.outputFileName || `export-${job.id}`;
    triggerDownload(blob, fileName);
    setUnreadIds((prev) => prev.filter((id) => id !== job.id));
  }, [authHeaders]);

  const cancelJob = useCallback(async (job) => {
    if (!job?.id || (job.status !== "QUEUED" && job.status !== "PROCESSING")) return;
    const res = await fetch(`${API_BASE}/api/export-jobs/${encodeURIComponent(job.id)}/cancel`, {
      method: "POST",
      headers: authHeaders(),
    });
    if (!res.ok) return;
    const updated = await res.json();
    previousJobsRef.current.set(updated.id, updated);
    setJobs((prev) => [updated, ...prev.filter((item) => item.id !== updated.id)]);
    setUnreadIds((prev) => Array.from(new Set([updated.id, ...prev])));
  }, [authHeaders]);

  return (
    <div className="shell-utility" ref={menuRef}>
      <div className="shell-utility-actions">
        <div className="shell-utility-notifications">
          <button
            type="button"
            className={`shell-utility-trigger shell-notification-trigger ${notificationsOpen ? "active" : ""}`}
            onClick={() => {
              setNotificationsOpen((value) => {
                const next = !value;
                if (next && unreadIds.length > 0) {
                  setUnreadIds([]);
                }
                return next;
              });
              setOpen(false);
            }}
            title="Notifications"
            aria-label="Open notifications"
            aria-expanded={notificationsOpen}
          >
            <span className="shell-utility-trigger-icon">
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <path d="M15 17h5l-1.4-1.4A2 2 0 0 1 18 14.2V11a6 6 0 1 0-12 0v3.2a2 2 0 0 1-.6 1.4L4 17h5" />
                <path d="M9 17a3 3 0 0 0 6 0" />
              </svg>
            </span>
            {notificationCount > 0 && <span className="shell-utility-trigger-badge">{notificationCount}</span>}
          </button>

          {notificationsOpen && (
            <aside className="shell-notification-panel">
              <div className="shell-export-flyout-header">
                <span>Notifications</span>
                <div className="shell-export-flyout-head-actions">
                  <span className="shell-export-flyout-pill">{notifications.length}</span>
                </div>
              </div>
              <div className="shell-export-flyout-list">
                {notifications.length > 0 ? notifications.map((item) => (
                  <div key={item.id} className="shell-utility-notification-item shell-export-flyout-item">
                    <span className={`shell-utility-notification-dot status-${(item.status || "").toLowerCase()}`} />
                    <div className="shell-utility-notification-body">
                      <p>{item.title}</p>
                      <small>{item.time}</small>
                      <div className="shell-utility-notification-meta">{item.detail}</div>
                      {(item.status === "PROCESSING" || item.status === "QUEUED") && (
                        <>
                          <div className="shell-utility-notification-progress">
                            <div className="shell-utility-notification-progress-fill" style={{ width: `${item.progressPercentage || 0}%` }} />
                          </div>
                          <div className="shell-utility-notification-subtext">{item.progressPercentage || 0}% complete</div>
                        </>
                      )}
                      {item.status === "FAILED" && item.errorMessage && (
                        <div className="shell-utility-notification-error">{item.errorMessage}</div>
                      )}
                      {item.status === "CANCELLED" && item.errorMessage && (
                        <div className="shell-utility-notification-subtext">{item.errorMessage}</div>
                      )}
                      {(item.status === "QUEUED" || item.status === "PROCESSING") && (
                        <button
                          type="button"
                          className="shell-utility-cancel-btn"
                          onClick={() => cancelJob(item)}
                        >
                          Cancel export
                        </button>
                      )}
                      {item.downloadReady && (
                        <button
                          type="button"
                          className="shell-utility-download-btn"
                          onClick={() => downloadJob(item)}
                        >
                          Download
                        </button>
                      )}
                    </div>
                  </div>
                )) : (
                  <div className="shell-notification-empty">
                    No export notifications yet.
                  </div>
                )}
              </div>
            </aside>
          )}
        </div>

        <button
          type="button"
          className={`shell-utility-trigger ${open ? "active" : ""}`}
          onClick={() => {
            setOpen((value) => !value);
            setNotificationsOpen(false);
          }}
          title="Quick access"
          aria-label="Open quick access menu"
          aria-expanded={open}
        >
          <span className="shell-utility-trigger-icon">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <line x1="4" y1="6" x2="20" y2="6" />
              <line x1="4" y1="12" x2="20" y2="12" />
              <line x1="4" y1="18" x2="20" y2="18" />
              <circle cx="9" cy="6" r="1.5" fill="currentColor" stroke="none" />
              <circle cx="15" cy="12" r="1.5" fill="currentColor" stroke="none" />
              <circle cx="11" cy="18" r="1.5" fill="currentColor" stroke="none" />
            </svg>
          </span>
          <svg className="shell-utility-trigger-caret" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2">
            <polyline points="6 9 12 15 18 9" />
          </svg>
        </button>
      </div>

      {open && (
        <div className="shell-utility-menu">
          <div className="shell-utility-menu-header">
            <div>
              <p className="shell-utility-menu-title">Quick Access</p>
              <p className="shell-utility-menu-subtitle">Session and account</p>
            </div>
          </div>

          <div className="shell-utility-option-list" role="tablist" aria-label="Quick access sections">
            <button
              type="button"
              className={`shell-utility-option ${activeSection === "session" ? "active" : ""}`}
              onClick={() => setActiveSection("session")}
            >
              <span className="shell-utility-option-icon session">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                  <circle cx="12" cy="12" r="9" />
                  <polyline points="12 7 12 12 16 14" />
                </svg>
              </span>
              <span className="shell-utility-option-text">
                <span>Session</span>
                <small>{sessionState.value || sessionState.label}</small>
              </span>
            </button>

            <button
              type="button"
              className={`shell-utility-option ${activeSection === "account" ? "active" : ""}`}
              onClick={() => setActiveSection("account")}
            >
              <span className="shell-utility-option-icon account">
                <div className="shell-utility-mini-avatar">{initials}</div>
              </span>
              <span className="shell-utility-option-text">
                <span>Account</span>
                <small>{user?.role || "Profile"}</small>
              </span>
            </button>
          </div>

          <div className="shell-utility-panel">
            {activeSection === "session" && (
              <div className="shell-utility-session-plain">
                <div className="shell-utility-section-head compact">
                  <span>Session status</span>
                </div>
                <div className="shell-utility-session-line">
                  <span className="shell-utility-session-key">State</span>
                  <span
                    className="shell-utility-session-chip"
                    style={{ "--utility-session-accent": sessionState.accentColor }}
                  >
                    {sessionState.label}
                  </span>
                </div>
                <div className="shell-utility-session-line">
                  <span className="shell-utility-session-key">Time left</span>
                  <span className="shell-utility-session-inline-value">{sessionState.value || "Unavailable"}</span>
                </div>
                <div className="shell-utility-session-note">{sessionState.detail}</div>
              </div>
            )}

            {activeSection === "account" && (
              <div className="shell-utility-account">
                <div className="shell-utility-account-card">
                  <div className="shell-utility-account-avatar">{initials}</div>
                  <div className="shell-utility-account-meta">
                    <p>{user?.name}</p>
                    <span>{user?.role}</span>
                    <small>{user?.employeeId}</small>
                  </div>
                </div>

                <div className="shell-utility-account-actions">
                  <button
                    type="button"
                    className="shell-utility-action-btn"
                    onClick={() => {
                      setOpen(false);
                      onOpenTab("profile");
                    }}
                  >
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                      <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2" />
                      <circle cx="12" cy="7" r="4" />
                    </svg>
                    My Profile
                  </button>
                  <button
                    type="button"
                    className="shell-utility-action-btn logout"
                    onClick={() => {
                      setOpen(false);
                      logout();
                    }}
                  >
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                      <path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4" />
                      <polyline points="16 17 21 12 16 7" />
                      <line x1="21" y1="12" x2="9" y2="12" />
                    </svg>
                    Sign Out
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
