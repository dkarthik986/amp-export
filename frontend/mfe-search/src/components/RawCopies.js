import React, { useState, useEffect, useCallback, useMemo } from "react";
import "./RawCopies.css";
import { useAuth } from "../AuthContext";

const API_BASE = `${process.env.REACT_APP_API_BASE_URL}/api/raw-copies`;

// ── Helpers ───────────────────────────────────────────────────────────────────

const statusCls = (s) => {
    if (!s) return "rc-status rc-status-default";
    const u = s.toUpperCase();
    if (u.includes("OK") || u.includes("DISTRIBUTED") || u.includes("ACCEPTED") || u.includes("DELIVERED"))
        return "rc-status rc-status-ok";
    if (u.includes("PENDING") || u.includes("PROCESSING") || u.includes("PROGRESS"))
        return "rc-status rc-status-pending";
    if (u.includes("FAIL") || u.includes("REJECT") || u.includes("ERROR"))
        return "rc-status rc-status-fail";
    return "rc-status rc-status-default";
};

const dirCls = (d) => {
    if (!d) return "rc-dir rc-dir-rt";
    const u = d.toUpperCase();
    if (u === "INBOUND")  return "rc-dir rc-dir-in";
    if (u === "OUTBOUND") return "rc-dir rc-dir-out";
    return "rc-dir rc-dir-rt";
};

const fmtDate = (s) => {
    if (!s) return "—";
    try { return new Date(s).toLocaleString("en-US", { year:"numeric", month:"2-digit", day:"2-digit", hour:"2-digit", minute:"2-digit", second:"2-digit", hour12: false }); }
    catch { return s; }
};

// ── RawCopies Component ───────────────────────────────────────────────────────
export default function RawCopies() {
    const { token } = useAuth();

    const authHeaders = useCallback(() => ({
        "Content-Type": "application/json",
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
    }), [token]);

    // ── Search state ────────────────────────────────────────────────────────
    const [filters, setFilters] = useState({
        messageReference: "",
        messageId: "",
        sender: "",
        receiver: "",
        messageTypeCode: "",
        direction: "",
        currentStatus: "",
        protocol: "",
        inputType: "",
        source: "",
        isDuplicate: "",   // "" | "true" | "false"
        startDate: "",
        endDate: "",
        freeText: "",
    });

    const [opts,        setOpts]        = useState({ messageTypeCodes:[], directions:[], statuses:[], protocols:[], inputTypes:[], sources:[] });
    const [optsLoading, setOptsLoading] = useState(true);
    const [results,     setResults]     = useState([]);
    const [total,       setTotal]       = useState(0);
    const [totalPages,  setTotalPages]  = useState(0);
    const [page,        setPage]        = useState(0);
    const [pageSize]                    = useState(20);
    const [loading,     setLoading]     = useState(false);
    const [error,       setError]       = useState(null);
    const [searched,    setSearched]    = useState(false);
    const [collapsed,   setCollapsed]   = useState(false);
    const [expandedRow, setExpandedRow] = useState(null);  // _id of expanded raw XML row
    const [copiedId,    setCopiedId]    = useState(null);

    // ── Load dropdown options ───────────────────────────────────────────────
    useEffect(() => {
        if (!token) return;
        fetch(`${API_BASE}/dropdown-options`, { headers: authHeaders() })
            .then(r => r.json())
            .then(d => { setOpts(d.data || d); setOptsLoading(false); })
            .catch(() => setOptsLoading(false));
    }, [token, authHeaders]);

    const set = (key) => (e) => setFilters(f => ({ ...f, [key]: e.target.value }));

    // ── Execute search ──────────────────────────────────────────────────────
    const doSearch = useCallback((p = 0) => {
        if (!token) return;
        setLoading(true); setError(null);

        const params = new URLSearchParams({ page: p, size: pageSize });
        Object.entries(filters).forEach(([k, v]) => { if (v !== "") params.set(k, v); });

        fetch(`${API_BASE}?${params}`, { headers: authHeaders() })
            .then(r => { if (!r.ok) throw new Error(`Error ${r.status}`); return r.json(); })
            .then(data => {
                const rows = data.content || data.rows || [];
                setResults(rows);
                setTotal(data.totalElements || rows.length);
                setTotalPages(data.totalPages || 1);
                setPage(p);
                setSearched(true);
                setLoading(false);
                if (!collapsed && rows.length > 0) setCollapsed(true);
            })
            .catch(e => { setError(e.message); setLoading(false); });
    }, [token, authHeaders, filters, pageSize, collapsed]);

    const handleReset = () => {
        setFilters({ messageReference:"", messageId:"", sender:"", receiver:"", messageTypeCode:"",
            direction:"", currentStatus:"", protocol:"", inputType:"", source:"",
            isDuplicate:"", startDate:"", endDate:"", freeText:"" });
        setResults([]); setTotal(0); setTotalPages(0); setPage(0); setSearched(false);
        setExpandedRow(null); setError(null); setCollapsed(false);
    };

    const handleKey = (e) => { if (e.key === "Enter") doSearch(0); };

    // ── Group results by messageReference ───────────────────────────────────
    const grouped = useMemo(() => {
        const map = new Map();
        results.forEach(row => {
            const ref = row.messageReference || "—";
            if (!map.has(ref)) map.set(ref, []);
            map.get(ref).push(row);
        });
        return [...map.entries()]; // [[ref, [rows...]], ...]
    }, [results]);

    // ── Copy raw XML ────────────────────────────────────────────────────────
    const copyRaw = (id, text) => {
        navigator.clipboard.writeText(text || "").then(() => {
            setCopiedId(id);
            setTimeout(() => setCopiedId(null), 1800);
        });
    };

    // ── Summary stats ───────────────────────────────────────────────────────
    const dupCount  = results.filter(r => r.isDuplicate).length;
    const okCount   = results.filter(r => statusCls(r.currentStatus).includes("ok")).length;

    // ── DynSelect helper ────────────────────────────────────────────────────
    const Sel = ({ stateKey, placeholder, options }) => (
        <select value={filters[stateKey]} onChange={set(stateKey)} onKeyDown={handleKey} disabled={optsLoading}>
            <option value="">{optsLoading ? "Loading…" : placeholder}</option>
            {options.map(o => <option key={o} value={o}>{o}</option>)}
        </select>
    );

    return (
        <div>
            {/* ── Search panel ── */}
            <div className="rc-panel">
                <div className="rc-panel-title" onClick={() => setCollapsed(p => !p)}>
                    <div className="rc-panel-title-left">
                        <span>Raw Copies Search</span>
                        <span className="rc-mode-chip">
                            <svg width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5">
                                <path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/>
                                <polyline points="14 2 14 8 20 8"/>
                            </svg>
                            amp_raw_copies
                        </span>
                        {total > 0 && <span className="filter-badge">{total.toLocaleString()} found</span>}
                    </div>
                    <span className="collapse-icon">{collapsed ? "▼ Expand" : "▲ Collapse"}</span>
                </div>

                {!collapsed && (
                    <div className="rc-grid">
                        {/* Row 1 — References */}
                        <div className="rc-field">
                            <label>Message Reference</label>
                            <input placeholder="e.g. KCRJ48066072DGTF" value={filters.messageReference} onChange={set("messageReference")} onKeyDown={handleKey}/>
                        </div>
                        <div className="rc-field">
                            <label>Message ID</label>
                            <input placeholder="Internal message ID" value={filters.messageId} onChange={set("messageId")} onKeyDown={handleKey}/>
                        </div>
                        <div className="rc-field">
                            <label>Sender BIC</label>
                            <input placeholder="e.g. ABNANL2AXXX" value={filters.sender} onChange={set("sender")} onKeyDown={handleKey}/>
                        </div>
                        <div className="rc-field">
                            <label>Receiver BIC</label>
                            <input placeholder="e.g. RBOSGB2LXXX" value={filters.receiver} onChange={set("receiver")} onKeyDown={handleKey}/>
                        </div>

                        {/* Row 2 — Classification */}
                        <div className="rc-field">
                            <label>Message Type</label>
                            <Sel stateKey="messageTypeCode" placeholder="All Types" options={opts.messageTypeCodes || []}/>
                        </div>
                        <div className="rc-field">
                            <label>Direction</label>
                            <Sel stateKey="direction" placeholder="All Directions" options={opts.directions || []}/>
                        </div>
                        <div className="rc-field">
                            <label>Status</label>
                            <Sel stateKey="currentStatus" placeholder="All Statuses" options={opts.statuses || []}/>
                        </div>
                        <div className="rc-field">
                            <label>Protocol</label>
                            <Sel stateKey="protocol" placeholder="All Protocols" options={opts.protocols || []}/>
                        </div>
                        <div className="rc-field">
                            <label>Input Type</label>
                            <Sel stateKey="inputType" placeholder="All Types" options={opts.inputTypes || []}/>
                        </div>
                        <div className="rc-field">
                            <label>Source</label>
                            <Sel stateKey="source" placeholder="All Sources" options={opts.sources || []}/>
                        </div>

                        {/* Date range */}
                        <div className="rc-field">
                            <label>Received From</label>
                            <input type="date" value={filters.startDate} onChange={set("startDate")} onKeyDown={handleKey}/>
                        </div>
                        <div className="rc-field">
                            <label>Received To</label>
                            <input type="date" value={filters.endDate} onChange={set("endDate")} onKeyDown={handleKey}/>
                        </div>

                        {/* Duplicate flag */}
                        <div className="rc-field">
                            <label>Duplicate</label>
                            <select value={filters.isDuplicate} onChange={set("isDuplicate")} onKeyDown={handleKey}>
                                <option value="">All</option>
                                <option value="true">Duplicates only</option>
                                <option value="false">Non-duplicates only</option>
                            </select>
                        </div>

                        {/* Free text — full width */}
                        <div className="rc-field rc-field-wide">
                            <label>Free Search (across reference, sender, receiver, raw content)</label>
                            <input placeholder="Search across all fields…" value={filters.freeText} onChange={set("freeText")} onKeyDown={handleKey}/>
                        </div>
                    </div>
                )}
            </div>

            {/* ── Action bar ── */}
            <div className="action-bar">
                <div className="action-left">
                    <button className={`search-btn${loading ? " btn-loading" : ""}`} onClick={() => doSearch(0)} disabled={loading}>
                        {loading
                            ? <><span className="spinner"/>Searching…</>
                            : <><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round"><circle cx="11" cy="11" r="7"/><line x1="16.5" y1="16.5" x2="22" y2="22"/></svg>Search Raw Copies</>
                        }
                    </button>
                    <button className="clear-btn" onClick={handleReset}>
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round"><polyline points="1 4 1 10 7 10"/><path d="M3.51 15a9 9 0 102.13-9.36L1 10"/></svg>
                        Reset
                    </button>
                </div>
                <div className="action-hint">
                    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>
                    Click a row to expand raw XML · Results grouped by Message Reference
                </div>
            </div>

            {/* ── Error ── */}
            {error && (
                <div style={{padding:"10px 16px",background:"var(--danger-light)",borderRadius:6,marginBottom:8,fontSize:13,color:"var(--danger)",border:"1px solid var(--danger-border)"}}>
                    ⚠ {error}
                </div>
            )}

            {/* ── Results ── */}
            {searched && (
                <>
                    {/* Stats bar */}
                    {results.length > 0 && (
                        <div className="rc-stats-bar">
                            <div className="rc-stat">
                                <span className="rc-stat-value">{total.toLocaleString()}</span>
                                <span className="rc-stat-label">Total Raw Copies</span>
                            </div>
                            <div className="rc-stat">
                                <span className="rc-stat-value">{grouped.length}</span>
                                <span className="rc-stat-label">Unique References</span>
                            </div>
                            <div className="rc-stat rc-stat-dup">
                                <span className="rc-stat-value">{dupCount}</span>
                                <span className="rc-stat-label">Duplicates</span>
                            </div>
                            <div className="rc-stat rc-stat-ok">
                                <span className="rc-stat-value">{okCount}</span>
                                <span className="rc-stat-label">Distributed OK</span>
                            </div>
                        </div>
                    )}

                    {/* Table */}
                    {results.length === 0 ? (
                        <div className="rc-empty">
                            <svg width="44" height="44" viewBox="0 0 24 24" fill="none" stroke="var(--gray-4)" strokeWidth="1.5"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
                            <p>No raw copies found</p>
                            <span>Try adjusting your search filters</span>
                        </div>
                    ) : (
                        <div className="rc-table-wrap">
                            <table className="rc-table">
                                <thead>
                                    <tr>
                                        <th style={{width:36}}/>
                                        <th>#</th>
                                        <th>Message Reference</th>
                                        <th>Message ID</th>
                                        <th>Type</th>
                                        <th>Direction</th>
                                        <th>Status</th>
                                        <th>Sender</th>
                                        <th>Receiver</th>
                                        <th>Protocol</th>
                                        <th>Duplicate</th>
                                        <th>Received At</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {grouped.map(([ref, rows]) => (
                                        <React.Fragment key={ref}>
                                            {/* Group header */}
                                            <tr className="rc-group-header">
                                                <td colSpan={12}>
                                                    <div className="rc-group-ref">
                                                        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="#1d4ed8" strokeWidth="2.5"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>
                                                        <span className="rc-group-ref-text">{ref}</span>
                                                        <span className="rc-group-badge">
                                                            <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg>
                                                            {rows.length} cop{rows.length === 1 ? "y" : "ies"}
                                                        </span>
                                                        {rows.some(r => r.isDuplicate) && (
                                                            <span style={{fontSize:10,fontWeight:700,color:"#dc2626",background:"#fee2e2",padding:"2px 7px",borderRadius:20}}>
                                                                HAS DUPLICATES
                                                            </span>
                                                        )}
                                                    </div>
                                                </td>
                                            </tr>

                                            {/* Individual copy rows */}
                                            {rows.map((row, ri) => (
                                                <React.Fragment key={row.id || ri}>
                                                    <tr onClick={() => setExpandedRow(p => p === row.id ? null : row.id)}
                                                        style={{background: ri % 2 === 0 ? "var(--white)" : "var(--gray-7)"}}>
                                                        <td>
                                                            <button
                                                                className={`rc-expand-btn${expandedRow === row.id ? " open" : ""}`}
                                                                onClick={e => { e.stopPropagation(); setExpandedRow(p => p === row.id ? null : row.id); }}
                                                                title="View raw content"
                                                            >
                                                                <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><polyline points="9 18 15 12 9 6"/></svg>
                                                            </button>
                                                        </td>
                                                        <td style={{color:"var(--gray-3)",fontWeight:600,fontSize:12}}>{ri + 1}</td>
                                                        <td className="rc-mono" style={{color:"#2563eb"}}>{row.messageReference || "—"}</td>
                                                        <td className="rc-mono" style={{fontSize:11}}>{row.messageId ? row.messageId.slice(0, 20) + (row.messageId.length > 20 ? "…" : "") : "—"}</td>
                                                        <td><span style={{fontFamily:"monospace",fontWeight:700,fontSize:12}}>{row.messageTypeCode || "—"}</span></td>
                                                        <td><span className={dirCls(row.direction)}>{row.direction || "—"}</span></td>
                                                        <td><span className={statusCls(row.currentStatus)}>{row.currentStatus || "—"}</span></td>
                                                        <td className="rc-mono">{row.senderAddress || "—"}</td>
                                                        <td className="rc-mono">{row.receiverAddress || "—"}</td>
                                                        <td style={{fontSize:12}}>{row.protocol || "—"}</td>
                                                        <td>
                                                            {row.isDuplicate === true  && <span className="rc-dup-yes">● DUP</span>}
                                                            {row.isDuplicate === false && <span className="rc-dup-no">● NO</span>}
                                                            {row.isDuplicate == null   && <span style={{color:"var(--gray-4)"}}>—</span>}
                                                        </td>
                                                        <td style={{fontSize:12,whiteSpace:"nowrap"}}>{fmtDate(row.receivedAt || row.ampDateReceived)}</td>
                                                    </tr>

                                                    {/* Expanded raw XML */}
                                                    {expandedRow === row.id && (
                                                        <tr className="rc-raw-row">
                                                            <td colSpan={12}>
                                                                <div className="rc-raw-inner">
                                                                    <div style={{display:"flex",alignItems:"center",justifyContent:"space-between"}}>
                                                                        <div>
                                                                            <div className="rc-raw-label">Raw Input — {row.inputType || "UNKNOWN"} · {row.source || "—"}</div>
                                                                            <div style={{fontSize:10,color:"#475569",marginTop:2}}>
                                                                                ID: <span style={{fontFamily:"monospace"}}>{row.id}</span>
                                                                                {row.ampDateReceived && <> · AMP received: {fmtDate(row.ampDateReceived)}</>}
                                                                            </div>
                                                                        </div>
                                                                        {row.rawInput && (
                                                                            <button
                                                                                className={`rc-copy-btn${copiedId === row.id ? " copied" : ""}`}
                                                                                onClick={e => { e.stopPropagation(); copyRaw(row.id, row.rawInput); }}
                                                                            >
                                                                                {copiedId === row.id ? "✓ Copied" : "Copy XML"}
                                                                            </button>
                                                                        )}
                                                                    </div>
                                                                    {row.rawInput ? (
                                                                        <pre className="rc-raw-xml">{row.rawInput}</pre>
                                                                    ) : (
                                                                        <div style={{color:"#64748b",fontStyle:"italic",fontSize:13}}>No raw content available</div>
                                                                    )}
                                                                </div>
                                                            </td>
                                                        </tr>
                                                    )}
                                                </React.Fragment>
                                            ))}
                                        </React.Fragment>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    )}

                    {/* Pagination */}
                    {totalPages > 1 && (
                        <div className="rc-pagination">
                            <span className="rc-page-info">
                                Showing <strong>{page * pageSize + 1}–{Math.min((page + 1) * pageSize, total)}</strong> of <strong>{total.toLocaleString()}</strong>
                            </span>
                            <div className="rc-page-btns">
                                <button className="rc-pg-btn" onClick={() => doSearch(0)} disabled={page === 0}>««</button>
                                <button className="rc-pg-btn" onClick={() => doSearch(page - 1)} disabled={page === 0}>‹</button>
                                {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
                                    const p = Math.max(0, Math.min(page - 2, totalPages - 5)) + i;
                                    return (
                                        <button key={p} className={`rc-pg-btn${page === p ? " rc-pg-active" : ""}`} onClick={() => doSearch(p)}>
                                            {p + 1}
                                        </button>
                                    );
                                })}
                                <button className="rc-pg-btn" onClick={() => doSearch(page + 1)} disabled={page >= totalPages - 1}>›</button>
                                <button className="rc-pg-btn" onClick={() => doSearch(totalPages - 1)} disabled={page >= totalPages - 1}>»»</button>
                            </div>
                        </div>
                    )}
                </>
            )}
        </div>
    );
}