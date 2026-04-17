import React, { useState, useRef, useEffect, useCallback, useMemo } from "react";
import "./Search.css";
import "./RawCopies.css";
import { useAuth } from "../AuthContext";

// ── All API endpoints read from .env — no hardcoded URLs ─────────────────────
const API_BASE_URL      = `${process.env.REACT_APP_API_BASE_URL}/api/search`;
const API_EXPORT_ALL_URL = `${process.env.REACT_APP_API_BASE_URL}/api/search/export-all`;
const API_EXPORT_ALL_FILE_URL = `${process.env.REACT_APP_API_BASE_URL}/api/search/export-all/file`;
const API_EXPORT_JOBS_URL = `${process.env.REACT_APP_API_BASE_URL}/api/export-jobs`;
const API_DETAIL_BY_REF_URL = `${process.env.REACT_APP_API_BASE_URL}/api/search/detail/by-reference`;
const API_DETAILS_BY_REFS_URL = `${process.env.REACT_APP_API_BASE_URL}/api/search/details/by-references`;
const API_DROPDOWN_URL   = `${process.env.REACT_APP_API_BASE_URL}/api/dropdown-options`;
const API_FIELD_CFG_URL  = `${process.env.REACT_APP_API_BASE_URL}/api/search/field-config`;
const API_RAW_COPIES_URL = `${process.env.REACT_APP_API_BASE_URL}/api/raw-copies`;
const EXPORT_JOB_REFRESH_EVENT = "swift-export-job-created";

// ── MT/MX pair map ────────────────────────────────────────────────────────────
const BASE_MT_MX_PAIRS = {
    "MT103/pacs.008": ["MT103", "pacs.008"],
    "MT199/pacs.002": ["MT199", "pacs.002"],
    "MT202/pacs.009": ["MT202", "pacs.009"],
    "MT700/pain.001": ["MT700", "pain.001"],
    "MT940/camt.053": ["MT940", "camt.053"],
};
let allMtMxTypeMap = { ...BASE_MT_MX_PAIRS };

// Bug #7 fix: previous code used new Date(y, m, d) where m came from splitting
// "YYYY/MM/DD" — making it 1-indexed while Date() expects 0-indexed months.
// For end-of-month dates this caused overflow: Jan 31 → Mar 3 instead of Feb 28.
const addOneMonth = (dateStr) => {
    if (!dateStr) return "";
    const [y, m, d] = dateStr.split("/").map(Number);
    // Advance month correctly: m is 1-indexed, convert to 0-indexed for Date(), then +1
    let ny = y, nm = m; // still 1-indexed
    if (nm === 12) { ny += 1; nm = 1; } else { nm += 1; }
    // Clamp day to actual days in target month
    const maxDay = new Date(ny, nm, 0).getDate(); // day 0 of next month = last day of nm
    const nd = Math.min(d, maxDay);
    return `${ny}/${String(nm).padStart(2, "0")}/${String(nd).padStart(2, "0")}`;
};
const clampToOneMonth = (startStr, endStr) => {
    if (!startStr || !endStr) return endStr;
    const maxEnd = addOneMonth(startStr);
    return endStr > maxEnd ? maxEnd : endStr;
};
const normalizeFormat = (rawFormat) => {
    if (!rawFormat) return rawFormat;
    return rawFormat.replace("ALL-MT&MX", "ALL MT&MX");
};
const buildAllMtMxTypeMap = (backendPairs) => {
    const map = { ...BASE_MT_MX_PAIRS };
    if (backendPairs && backendPairs.length > 0) {
        backendPairs.forEach(label => {
            if (!map[label]) {
                const parts = label.split("/");
                if (parts.length === 2) map[label] = [parts[0].trim(), parts[1].trim()];
            }
        });
    }
    return map;
};
const getDisplayFormat = (msg) => normalizeFormat(msg.format);
const getDisplayType = (msg) => {
    if (normalizeFormat(msg.format) === "ALL MT&MX") {
        for (const [pairedLabel, individualTypes] of Object.entries(allMtMxTypeMap)) {
            if (individualTypes.includes(msg.type)) return pairedLabel;
        }
    }
    return msg.type;
};
const formatDirection = (val) => {
    if (!val) return "—";
    const v = String(val).trim().toUpperCase();
    if (v === "I") return "INCOMING";
    if (v === "O") return "OUTGOING";
    return v;
};
const dirClass = (val) => {
    if (!val) return "";
    const v = String(val).trim().toUpperCase();
    if (v === "I" || v === "INCOMING") return "dir-incoming";
    if (v === "O" || v === "OUTGOING") return "dir-outgoing";
    return "";
};
// Bug #9 fix: previous lookup used generic status names (ACCEPTED/REJECTED) but
// the backend returns domain-specific names (FinalAcknowledged/FinalRejected).
// None matched → every row in the result table was unstyled (empty class).
const statusCls = (s) => {
    if (!s) return "";
    const u = s.toUpperCase();
    if (["ACCEPTED","DELIVERED","FINALACKNOWLEDGED","FINALDISTRIBUTEDOK",
         "FINALDISTRIBUTEDPARTIAL"].includes(u)) return "badge-ok";
    if (["PENDING","PROCESSING","REPAIR","INTERMEDIATEPROCESSING"].includes(u)) return "badge-pending";
    if (["REJECTED","FAILED","FINALREJECTED"].includes(u)) return "badge-bypass";
    return "";
};

// ── Raw Copies helpers ────────────────────────────────────────────────────────
const rcStatusCls = (s) => {
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
const rcDirCls = (d) => {
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

function highlight(text, search) {
    if (!search || !text) return text ?? "—";
    const str = String(text);
    const idx = str.toLowerCase().indexOf(search.toLowerCase());
    if (idx === -1) return str;
    return <>{str.slice(0, idx)}<mark className="hl">{str.slice(idx, idx + search.length)}</mark>{str.slice(idx + search.length)}</>;
}

const toPrettyLabel = (key) => {
    if (!key) return "";
    return String(key)
        .replace(/([a-z])([A-Z])/g, "$1 $2")
        .replace(/[._-]+/g, " ")
        .replace(/\s+/g, " ")
        .trim()
        .replace(/\b\w/g, ch => ch.toUpperCase());
};

const stringifyExportValue = (value) => {
    if (value === null || value === undefined) return "";
    if (typeof value === "string") return value;
    if (typeof value === "number" || typeof value === "boolean") return String(value);
    if (value instanceof Date) return value.toISOString();
    try { return JSON.stringify(value); } catch { return String(value); }
};

const HEADER_DYNAMIC_ORDER = [
    "messageTypeCode", "messageTypeName", "messageTypeDescription", "messageFormat",
    "messageReference", "transactionReference", "dateCreated", "dateReceived",
    "direction", "protocol", "service", "profileCode", "owner", "originatorApplication",
    "networkPriority", "processPriority", "processingType", "workflow", "workflowModel",
    "backendChannel", "networkChannel", "pdeIndication",
];
const HEADER_PARTY_KEYS = new Set(["senderAddress", "senderName", "receiverAddress", "receiverName"]);
const DETAIL_SKIP_PATHS = new Set(["_class", "version"]);
const LEGACY_DETAIL_FIELDS = [["id","ID"],["messageType","FORMAT"],["messageCode","TYPE"],["messageTypeDescription","DESCRIPTION"],["io","DIRECTION"],["status","STATUS"],["phase","PHASE"],["action","ACTION"],["reason","REASON"],["statusMessage","STATUS MESSAGE"],["statusChangeSource","STATUS SOURCE"],["statusDecision","STATUS DECISION"],["reference","MESSAGE REF"],["transactionReference","TXN REF"],["mur","MUR (TAG 20)"],["sender","SENDER"],["receiver","RECEIVER"],["senderInstitutionName","SENDER NAME"],["receiverInstitutionName","RECEIVER NAME"],["amount","AMOUNT"],["ccy","CURRENCY"],["valueDate","VALUE DATE"],["networkProtocol","PROTOCOL"],["networkChannel","NETWORK CHANNEL"],["networkPriority","NETWORK PRIORITY"],["deliveryMode","DELIVERY MODE"],["service","SERVICE"],["backendChannel","BACKEND CHANNEL"],["backendChannelProtocol","CHANNEL PROTOCOL"],["workflow","WORKFLOW"],["workflowModel","WORKFLOW MODEL"],["owner","OWNER"],["processingType","PROCESSING TYPE"],["processPriority","PROCESS PRIORITY"],["profileCode","PROFILE CODE"],["originatorApplication","ORIGINATOR APP"],["sessionNumber","SESSION NO"],["sequenceNumber","SEQUENCE NO"],["creationDate","CREATED"],["receivedDT","RECEIVED"],["statusDate","STATUS DATE"],["valueDate","VALUE DATE"],["bankOperationCode","BANK OP CODE"],["detailsOfCharges","CHARGES"],["remittanceInfo","REMITTANCE"],["applicationId","APP ID"],["serviceId","SERVICE ID"],["logicalTerminalAddress","LOGICAL TERMINAL"],["messagePriority","MSG PRIORITY"],["pdeIndication","PDE"],["bulkType","BULK TYPE"],["nrIndicator","NR IND"],["channelCode","CHANNEL CODE"]];

const isPlainObject = (value) => value && typeof value === "object" && !Array.isArray(value) && !(value instanceof Date);
const unwrapMetaObject = (value) => {
    if (!isPlainObject(value)) return value;
    const keys = Object.keys(value);
    if (keys.length === 1 && (keys[0] === "$oid" || keys[0] === "$date" || keys[0] === "$numberLong")) {
        return value[keys[0]];
    }
    return value;
};

const buildObjectPairs = (source, preferredKeys = [], excludedKeys = new Set()) => {
    if (!isPlainObject(source)) return [];
    const orderedKeys = [...preferredKeys.filter(key => key in source), ...Object.keys(source).filter(key => !preferredKeys.includes(key))];
    return orderedKeys
        .filter(key => !excludedKeys.has(key))
        .map(key => ({ key, label: toPrettyLabel(key), val: unwrapMetaObject(source[key]) }))
        .filter(item => item.val !== undefined && item.val !== null && item.val !== "");
};

const getLegacyHeaderPairs = (msg, raw = {}) => [
    { key: "messageCode", label: "Message Code", val: msg.messageCode || getDisplayType(msg) },
    { key: "messageFormat", label: "Message Format", val: raw.messageFormat || getDisplayFormat(msg) },
    { key: "reference", label: "Reference", val: msg.reference },
    { key: "transactionReference", label: "Transaction Reference", val: msg.transactionReference },
    { key: "transferReference", label: "Transfer Reference", val: msg.transferReference },
    { key: "mur", label: "MUR", val: msg.mur || msg.userReference },
    { key: "creationDate", label: "Creation Date", val: msg.creationDate || msg.date },
    { key: "receivedDT", label: "Received", val: msg.receivedDT },
    { key: "remittanceInfo", label: "Remittance", val: msg.remittanceInfo },
    { key: "uetr", label: "UETR", val: msg.uetr },
    { key: "workflow", label: "Workflow", val: msg.workflow },
    { key: "environment", label: "Environment", val: msg.environment },
    { key: "statusMessage", label: "Status Message", val: msg.statusMessage },
].filter(item => item.val !== undefined && item.val !== null && item.val !== "");

const getHeaderPairs = (msg) => {
    const raw = msg.rawMessage || {};
    const header = raw.header || {};
    return [
        { key: "messageType", label: "Message Type", val: getDisplayType(msg) || msg.messageCode || header.messageTypeCode },
        { key: "protocol", label: "Protocol", val: header.protocol || msg.networkProtocol || msg.protocol },
        { key: "sender", label: "Sender", val: header.senderAddress || msg.sender },
        { key: "service", label: "Service", val: header.service || msg.service },
        { key: "receiver", label: "Receiver", val: header.receiverAddress || msg.receiver },
        { key: "messageReference", label: "Message Reference", val: header.messageReference || msg.reference || msg.messageReference },
        { key: "transactionReference", label: "Transaction Reference", val: header.transactionReference || msg.transactionReference },
    ].filter(item => item.val !== undefined && item.val !== null && item.val !== "");
};

const getHeaderExportPairs = (msg) => getHeaderPairs(msg);

const flattenObjectFields = (source, prefix = "", out = []) => {
    if (!isPlainObject(source)) return out;
    Object.entries(source).forEach(([key, rawValue]) => {
        const path = prefix ? `${prefix}.${key}` : key;
        if (DETAIL_SKIP_PATHS.has(key) || DETAIL_SKIP_PATHS.has(path)) return;
        const value = unwrapMetaObject(rawValue);
        if (value === undefined || value === null || value === "") return;
        if (isPlainObject(value)) {
            flattenObjectFields(value, path, out);
            return;
        }
        out.push({ key: path, label: toPrettyLabel(path), val: value });
    });
    return out;
};

const getLegacyDetailPairs = (msg, raw = {}) => {
    const shown = new Set();
    const ordered = [];
    LEGACY_DETAIL_FIELDS.forEach(([key, label]) => {
        const val = raw[key] ?? msg[key];
        if (val !== undefined && val !== null && val !== "") {
            ordered.push({ key, label, val });
            shown.add(key);
        }
    });
    Object.entries(raw).forEach(([key, value]) => {
        if (!shown.has(key) && key !== "mtPayload" && key !== "block4Fields" && key !== "rawFin" && value !== undefined && value !== null && value !== "") {
            ordered.push({ key, label: key.toUpperCase(), val: value });
        }
    });
    return ordered;
};

const getDetailPairs = (msg) => {
    const raw = msg.rawMessage || {};
    const flattened = flattenObjectFields(raw);
    return flattened.length ? flattened : getLegacyDetailPairs(msg, raw);
};

const getRawPayloadText = (msg) => {
    const raw = msg.rawMessage || {};
    return msg.rawFin || raw.mtPayload?.rawFin || raw.mtPayload?.rawBlock4 || raw.body?.rawPayload || "—";
};

const escapeCsvCell = (value) => `"${stringifyExportValue(value).replace(/"/g, "\"\"")}"`;
const safeFileNamePart = (value) => String(value || "").replace(/[\\/:*?"<>|]+/g, "_").slice(0, 70);
const extractFilenameFromDisposition = (value) => {
    if (!value) return "";
    const utf8Match = value.match(/filename\*=UTF-8''([^;]+)/i);
    if (utf8Match?.[1]) {
        try { return decodeURIComponent(utf8Match[1]); } catch { return utf8Match[1]; }
    }
    const plainMatch = value.match(/filename="?([^";]+)"?/i);
    return plainMatch?.[1] || "";
};

const triggerDownload = (blob, filename) => {
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    a.click();
    setTimeout(() => URL.revokeObjectURL(url), 0);
};

const EXPORT_FORMAT_OPTIONS = [
    { key: "csv", iconText: "CSV", iconClass: "export-icon-csv", name: "Comma Separated", ext: ".csv" },
    { key: "excel", iconText: "XLS", iconClass: "export-icon-xlsx", name: "Excel Workbook", ext: ".xlsx" },
    { key: "json", iconText: "JSON", iconClass: "export-icon-json", name: "JSON Data", ext: ".json" },
    { key: "pdf", iconText: "PDF", iconClass: "export-icon-pdf", name: "Portable Document", ext: ".pdf" },
    { key: "word", iconText: "DOC", iconClass: "export-icon-word", name: "Word Document", ext: ".doc" },
    { key: "xml", iconText: "XML", iconClass: "export-icon-xml", name: "XML Data", ext: ".xml" },
    { key: "txt", iconText: "TXT", iconClass: "export-icon-txt", name: "Plain Text", ext: ".txt" },
    { key: "rje", iconText: "RJE", iconClass: "export-icon-rje", name: "Remote Job Entry", ext: ".rje", rawPayloadOnly: true, mtOnly: true },
    { key: "dospcc", iconText: "DOS", iconClass: "export-icon-dospcc", name: "DOSPCC", ext: ".dos", rawPayloadOnly: true, mtOnly: true },
];

const MT_RAW_ONLY_EXPORT_FORMATS = new Set(["rje", "dospcc"]);
const RESULT_TABLE_EXPORT_FORMATS = new Set(["csv", "excel", "pdf"]);
const SERVER_STREAMED_TABLE_EXPORT_FORMATS = new Set(["csv", "excel"]);

const loadScriptOnce = (src, checkReady) => new Promise((resolve, reject) => {
    if (checkReady()) { resolve(); return; }
    const existing = document.querySelector(`script[src="${src}"]`);
    if (existing) {
        if (checkReady()) { resolve(); return; }
        existing.addEventListener("load", () => resolve(), { once: true });
        existing.addEventListener("error", () => reject(new Error(`Failed loading ${src}`)), { once: true });
        return;
    }
    const sc = document.createElement("script");
    sc.src = src;
    sc.async = true;
    sc.onload = () => resolve();
    sc.onerror = () => reject(new Error(`Failed loading ${src}`));
    document.head.appendChild(sc);
});

const escapeHtml = (value) => stringifyExportValue(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");

const htmlText = (value) => {
    const text = stringifyExportValue(value ?? "—") || "—";
    return escapeHtml(text).replace(/\r?\n/g, "<br />");
};

const buildWordHtml = (title, columns, rows) => {
    const th = columns.map(c => `<th>${escapeHtml(c.label)}</th>`).join("");
    const body = rows.map(row => `<tr>${columns.map(c => `<td>${htmlText(row[c.key])}</td>`).join("")}</tr>`).join("");
    return `
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8" />
<title>${escapeHtml(title)}</title>
<style>
body { font-family: Arial, sans-serif; padding: 16px; color: #111827; }
h2 { margin: 0 0 12px; font-size: 18px; }
table { border-collapse: collapse; width: 100%; font-size: 11pt; }
th, td { border: 1px solid #d1d5db; padding: 6px 8px; vertical-align: top; text-align: left; }
th { background: #f3f4f6; font-weight: 700; }
</style>
</head>
<body>
<h2>${escapeHtml(title)}</h2>
<table>
<thead><tr>${th}</tr></thead>
<tbody>${body}</tbody>
</table>
</body>
</html>`;
};

const triggerWordDownload = (html, fileBaseName) => {
    triggerDownload(new Blob(["\ufeff", html], { type: "application/msword;charset=utf-8" }), `${fileBaseName}.doc`);
};

const getWordRenderableColumns = (columns, rows) => {
    const filtered = (columns || []).filter((column) => {
        if (column.key === "index") return false;
        return (rows || []).some((row) => {
            const value = stringifyExportValue(row?.[column.key] ?? "—") || "—";
            return value && value !== "—";
        });
    });
    return filtered.length ? filtered : (columns || []).filter(column => column.key !== "index");
};

const buildWordComponentExportHtml = ({ title, documents }) => {
    const renderTable = ({ columns, rows, pairMode = false }) => {
        const header = columns.map((column) => `<th>${escapeHtml(column.label)}</th>`).join("");
        const body = (rows.length ? rows : [{}]).map((row) => (
            `<tr>${columns.map((column, index) => `
                <td${pairMode && index === 0 ? ' class="word-label-cell"' : ""}>${htmlText(row?.[column.key] ?? "—")}</td>
            `).join("")}</tr>`
        )).join("");
        return `
            <table class="word-table${pairMode ? " word-pair-table" : ""}">
                <thead><tr>${header}</tr></thead>
                <tbody>${body}</tbody>
            </table>
        `;
    };

    const renderSection = (section) => {
        const rawHtml = section.type === "raw"
            ? `<pre class="word-raw-block">${escapeHtml(section.rawText || "—")}</pre>`
            : "";
        if (section.type === "raw") {
            return `
                <section class="word-section">
                    <h2 class="word-section-title">${escapeHtml(section.label)}</h2>
                    ${rawHtml}
                </section>
            `;
        }

        return `
            <section class="word-section">
                <h2 class="word-section-title">${escapeHtml(section.label)}</h2>
                ${renderTable({ columns: section.columns, rows: section.rows, pairMode: section.type === "pair" })}
            </section>
        `;
    };

    const body = (documents || []).map((doc) => `
        <article class="word-message${doc.pageBreak ? " word-page-break" : ""}">
            ${doc.pageLabel ? `<div class="word-page-label">${escapeHtml(doc.pageLabel)}</div>` : ""}
            <div class="word-message-ref">Message Ref: ${escapeHtml(doc.messageRef || "—")}</div>
            ${doc.summaryLine ? `<div class="word-message-summary">${escapeHtml(doc.summaryLine)}</div>` : ""}
            <div class="word-message-rule"></div>
            ${(doc.sections || []).map(renderSection).join("")}
        </article>
    `).join("");

    return `
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8" />
<title>${escapeHtml(title || "Export")}</title>
<style>
@page { margin: 0.6in 0.45in; }
body { font-family: "Segoe UI", Arial, sans-serif; margin: 0; padding: 0; color: #111827; background: #ffffff; }
.word-page-break { page-break-before: always; }
.word-message { margin: 0; }
.word-page-label { text-align: right; font-size: 11pt; font-weight: 700; color: #153263; margin: 0 0 12px; }
.word-message-ref { font-size: 18pt; font-weight: 700; color: #111827; margin: 0 0 8px; }
.word-message-summary { font-size: 12pt; color: #64748b; margin: 0 0 14px; letter-spacing: 0.02em; }
.word-message-rule { border-top: 1px solid #cbd5e1; margin: 0 0 18px; }
.word-section { margin: 0 0 18px; page-break-inside: avoid; }
.word-section-title { margin: 0 0 10px; font-size: 15pt; font-weight: 700; color: #213863; page-break-after: avoid; }
.word-table { border-collapse: collapse; width: 100%; font-size: 11pt; table-layout: fixed; }
.word-table thead { display: table-header-group; }
.word-table tr { page-break-inside: avoid; }
.word-table th, .word-table td { border: 1px solid #dbe3ef; padding: 7px 10px; vertical-align: top; text-align: left; word-break: break-word; }
.word-table th { background: #f3f4f6; color: #374151; font-weight: 700; }
.word-table tbody tr:nth-child(even) td { background: #f8fafc; }
.word-pair-table th:first-child, .word-pair-table td:first-child { width: 32%; }
.word-label-cell { font-weight: 700; color: #64748b; }
.word-raw-block { margin: 0; padding: 14px 16px; border: 1px solid #e2e8f0; border-radius: 8px; background: #f8fafc; color: #111827; font-family: Consolas, "Courier New", monospace; font-size: 9.5pt; line-height: 1.55; white-space: pre-wrap; word-break: break-word; }
</style>
</head>
<body>
${body}
</body>
</html>`;
};

const buildStructuredSectionData = (block) => {
    const columns = Array.isArray(block?.columns) ? block.columns : [];
    const rows = Array.isArray(block?.rows) ? block.rows : [];
    const hasFieldValue = columns.some(col => col.key === "field") && columns.some(col => col.key === "value");

    if (hasFieldValue) {
        return rows.reduce((acc, row, index) => {
            const key = stringifyExportValue(row?.field || `Field ${index + 1}`) || `Field ${index + 1}`;
            acc[key] = row?.value ?? "—";
            return acc;
        }, {});
    }

    if (rows.length === 1) {
        const single = rows[0] || {};
        return columns.reduce((acc, column) => {
            if (column.key === "index" || column.label === "#") return acc;
            acc[column.label] = single[column.key] ?? "—";
            return acc;
        }, {});
    }

    return rows.map((row) => columns.reduce((acc, column) => {
        if (column.key === "index" || column.label === "#") return acc;
        acc[column.label] = row?.[column.key] ?? "—";
        return acc;
    }, {}));
};

const buildSectionJsonData = ({ targetKey, block, meta = {} }) => {
    if (targetKey === "rawpayload") {
        const rawValue = block?.rows?.[0]?.value ?? block?.rows?.[0]?.rawPayload ?? "â€”";
        return stringifyExportValue(rawValue || "â€”") || "â€”";
    }

    const structured = buildStructuredSectionData(block);
    if (targetKey === "header") {
        return {
            messageType: meta.messageType ?? "â€”",
            messageFormat: meta.messageFormat ?? "â€”",
            messageReference: meta.messageReference ?? "â€”",
            ...structured,
        };
    }

    return structured;
};

const isWordExportDisabledForTargets = (selectedTargets = []) => Array.isArray(selectedTargets) && selectedTargets.includes("rawcopies");

const getExportFormatOptions = ({ targetKey, includeMtOnly = false, selectedTargets = [] }) => EXPORT_FORMAT_OPTIONS.map(option => {
    const disabled = option.key === "word" && isWordExportDisabledForTargets(selectedTargets);
    const disabledReason = disabled ? "Word export is unavailable when Raw Copies is selected." : "";
    return { ...option, disabled, disabledReason };
}).filter(option => {
    if (targetKey === "table" && !RESULT_TABLE_EXPORT_FORMATS.has(option.key)) return false;
    if (option.rawPayloadOnly && targetKey !== "rawpayload") return false;
    if (option.mtOnly && !includeMtOnly) return false;
    return true;
});

const normalizeFinPayloadText = (value) => {
    const text = typeof value === "string" ? value : stringifyExportValue(value);
    return text.replace(/\r?\n/g, "\r\n").trim();
};

const isMtMessage = (msg) => {
    const format = normalizeFormat(msg?.format || msg?.messageFormat || msg?.rawMessage?.messageFormat || "");
    if (String(format).toUpperCase() === "MT") return true;
    const type = String(msg?.messageCode || msg?.type || "").trim().toUpperCase();
    if (/^MT\d{3}/.test(type)) return true;
    const rawPayload = getRawPayloadText(msg);
    return typeof rawPayload === "string" && rawPayload.trim().startsWith("{1:");
};

const collectMtRawPayloads = (messages = []) => {
    const payloads = [];
    let skippedNonMt = 0;
    let skippedMissing = 0;

    (messages || []).forEach((msg) => {
        if (!isMtMessage(msg)) {
            skippedNonMt += 1;
            return;
        }
        const rawPayload = normalizeFinPayloadText(getRawPayloadText(msg));
        if (!rawPayload || rawPayload === "—") {
            skippedMissing += 1;
            return;
        }
        payloads.push(rawPayload);
    });

    return {
        payloads,
        skippedNonMt,
        skippedMissing,
    };
};

const encodeAsciiBytes = (text) => {
    const bytes = new Uint8Array(text.length);
    for (let i = 0; i < text.length; i += 1) bytes[i] = text.charCodeAt(i) & 0xff;
    return bytes;
};

const concatUint8Arrays = (chunks) => {
    const total = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
    const out = new Uint8Array(total);
    let offset = 0;
    chunks.forEach((chunk) => {
        out.set(chunk, offset);
        offset += chunk.length;
    });
    return out;
};

const buildRjeContent = (payloads) => `${payloads.join("$")}$`;

const buildDosPccBytes = (payloads) => {
    const sectorSize = 512;
    const chunks = payloads.map((payload) => {
        const body = encodeAsciiBytes(payload);
        const framed = new Uint8Array(body.length + 2);
        framed[0] = 0x01; // SOH
        framed.set(body, 1);
        framed[framed.length - 1] = 0x03; // ETX

        const paddedLength = Math.ceil(framed.length / sectorSize) * sectorSize;
        const padded = new Uint8Array(paddedLength);
        padded.fill(0x20);
        padded.set(framed, 0);
        return padded;
    });

    return concatUint8Arrays(chunks);
};

const exportMtRawPayloads = ({ format, messages, fileBaseName }) => {
    const { payloads, skippedNonMt, skippedMissing } = collectMtRawPayloads(messages);
    if (payloads.length === 0) {
        throw new Error("RJE / DOSPCC export requires MT messages with raw payload data.");
    }

    if (format === "rje") {
        const content = buildRjeContent(payloads);
        triggerDownload(new Blob([content], { type: "text/plain;charset=us-ascii" }), `${fileBaseName}.rje`);
        return { exportedCount: payloads.length, skippedNonMt, skippedMissing };
    }

    if (format === "dospcc") {
        const bytes = buildDosPccBytes(payloads);
        triggerDownload(new Blob([bytes], { type: "application/octet-stream" }), `${fileBaseName}.dos`);
        return { exportedCount: payloads.length, skippedNonMt, skippedMissing };
    }

    throw new Error(`Unsupported MT raw export format: ${format}`);
};

const clampNumber = (value, min, max) => Math.min(max, Math.max(min, value));

const estimatePdfColumnWidths = (columns, rows) => {
    const sampledRows = rows.slice(0, 50);
    return columns.map(col => {
        const sampleLengths = sampledRows.map(row => {
            const text = stringifyExportValue(row?.[col.key]);
            return text
                .split(/\r?\n/)
                .reduce((longest, line) => Math.max(longest, line.trim().length), 0);
        });
        const maxLength = Math.max(col.label.length, ...sampleLengths, 8);
        const width = maxLength * 5.2 + 16;
        return clampNumber(width, 72, 180);
    });
};

const getPdfRepeatColumns = (columns) => {
    const preferredKeys = ["reference", "transactionReference", "mur", "sequenceNumber", "type"];
    const matched = preferredKeys
        .map(key => columns.findIndex(col => col.key === key))
        .filter(index => index >= 0)
        .slice(0, 2);

    if (matched.length > 0) return matched;
    if (columns.length > 10) return [0];
    return [];
};

const exportRawCopiesPdf = async ({ base, exportTitle, rows, columns, showPdfTitle = true }) => {
    await loadScriptOnce("https://cdnjs.cloudflare.com/ajax/libs/jspdf/2.5.1/jspdf.umd.min.js", () => !!window.jspdf?.jsPDF);
    await loadScriptOnce("https://cdnjs.cloudflare.com/ajax/libs/jspdf-autotable/3.8.2/jspdf.plugin.autotable.min.js", () => !!window.jspdf?.jsPDF?.API?.autoTable);

    const { jsPDF } = window.jspdf;
    const doc = new jsPDF({ orientation: "portrait", unit: "pt", format: "a4" });
    const margin = { top: 48, right: 34, bottom: 34, left: 34 };
    const pageWidth = doc.internal.pageSize.getWidth();
    const pageHeight = doc.internal.pageSize.getHeight();
    const contentWidth = pageWidth - margin.left - margin.right;
    const metaColumns = columns.filter(col => col.key !== "rawInput");
    const lineHeight = 8.5;
    const codePadding = 8;
    let cursorY = margin.top;

    const drawPageHeader = () => {
        if (showPdfTitle) {
            doc.setFont("helvetica", "bold");
            doc.setFontSize(12);
            doc.setTextColor(17, 24, 39);
            doc.text(exportTitle, margin.left, 28);
        }
        doc.setFont("helvetica", "normal");
        doc.setFontSize(8);
        doc.text(`Page ${doc.getNumberOfPages()}`, pageWidth - margin.right, 28, { align: "right" });
    };

    const resetPageCursor = () => {
        cursorY = margin.top;
        drawPageHeader();
    };

    const ensureSpace = (neededHeight) => {
        if (cursorY + neededHeight <= pageHeight - margin.bottom) return;
        doc.addPage();
        resetPageCursor();
    };

    resetPageCursor();

    rows.forEach((row, idx) => {
        const copyTitle = row.messageReference && row.messageReference !== "—"
            ? `Copy ${idx + 1} - ${row.messageReference}`
            : `Copy ${idx + 1}`;
        const metaPairs = metaColumns
            .map(col => [col.label, stringifyExportValue(row?.[col.key])])
            .filter(([, value]) => value !== "");

        ensureSpace(28);
        doc.setFont("helvetica", "bold");
        doc.setFontSize(10);
        doc.setTextColor(33, 56, 99);
        doc.text(copyTitle, margin.left, cursorY);
        cursorY += 10;

        doc.autoTable({
            startY: cursorY,
            margin,
            tableWidth: contentWidth,
            theme: "grid",
            head: [["Field", "Value"]],
            body: metaPairs.length > 0 ? metaPairs : [["Field", "—"]],
            styles: {
                fontSize: 8,
                cellPadding: { top: 4, right: 5, bottom: 4, left: 5 },
                overflow: "linebreak",
                valign: "top",
                lineColor: [226, 232, 240],
                lineWidth: 0.35,
            },
            headStyles: {
                fillColor: [33, 56, 99],
                textColor: [255, 255, 255],
                fontStyle: "bold",
                fontSize: 8,
            },
            columnStyles: {
                0: { cellWidth: 126, fontStyle: "bold" },
                1: { cellWidth: contentWidth - 126 },
            },
            didDrawPage: () => {
                drawPageHeader();
            },
        });

        cursorY = (doc.lastAutoTable?.finalY || cursorY) + 12;
        const rawInput = stringifyExportValue(row?.rawInput || "—");
        const wrappedLines = doc.splitTextToSize(rawInput, contentWidth - (codePadding * 2));

        ensureSpace(26);
        doc.setFont("helvetica", "bold");
        doc.setFontSize(9);
        doc.setTextColor(17, 24, 39);
        doc.text("Raw Input", margin.left, cursorY);
        cursorY += 8;

        let offset = 0;
        while (offset < wrappedLines.length) {
            ensureSpace(28);
            const availableHeight = pageHeight - margin.bottom - cursorY - (codePadding * 2);
            const linesPerPage = Math.max(1, Math.floor(availableHeight / lineHeight));
            const lineChunk = wrappedLines.slice(offset, offset + linesPerPage);
            const boxHeight = (lineChunk.length * lineHeight) + (codePadding * 2);

            doc.setFillColor(248, 250, 252);
            doc.setDrawColor(226, 232, 240);
            doc.roundedRect(margin.left, cursorY, contentWidth, boxHeight, 6, 6, "FD");
            doc.setFont("courier", "normal");
            doc.setFontSize(7);
            doc.setTextColor(17, 24, 39);
            doc.text(lineChunk, margin.left + codePadding, cursorY + codePadding + 5);

            cursorY += boxHeight + 10;
            offset += linesPerPage;

            if (offset < wrappedLines.length) {
                doc.addPage();
                resetPageCursor();
                doc.setFont("helvetica", "bold");
                doc.setFontSize(9);
                doc.setTextColor(17, 24, 39);
                doc.text(`Raw Input (cont.) - ${copyTitle}`, margin.left, cursorY);
                cursorY += 8;
            }
        }

        cursorY += 6;
        ensureSpace(12);
        doc.setDrawColor(203, 213, 225);
        doc.line(margin.left, cursorY, pageWidth - margin.right, cursorY);
        cursorY += 14;
    });

    doc.save(`${base}.pdf`);
};

const exportFieldPairsPdf = async ({ base, exportTitle, rows, columns, showPdfTitle = true }) => {
    await loadScriptOnce("https://cdnjs.cloudflare.com/ajax/libs/jspdf/2.5.1/jspdf.umd.min.js", () => !!window.jspdf?.jsPDF);
    await loadScriptOnce("https://cdnjs.cloudflare.com/ajax/libs/jspdf-autotable/3.8.2/jspdf.plugin.autotable.min.js", () => !!window.jspdf?.jsPDF?.API?.autoTable);

    const { jsPDF } = window.jspdf;
    const doc = new jsPDF({ orientation: "portrait", unit: "pt", format: "a4" });
    const margin = { top: 48, right: 34, bottom: 34, left: 34 };
    const pageWidth = doc.internal.pageSize.getWidth();
    const pageHeight = doc.internal.pageSize.getHeight();
    const contentWidth = pageWidth - margin.left - margin.right;
    const pairColumns = ["field", "value"];
    const metaColumns = columns.filter(col => !pairColumns.includes(col.key));
    let cursorY = margin.top;

    const drawPageHeader = () => {
        if (showPdfTitle) {
            doc.setFont("helvetica", "bold");
            doc.setFontSize(12);
            doc.setTextColor(17, 24, 39);
            doc.text(exportTitle, margin.left, 28);
        }
        doc.setFont("helvetica", "normal");
        doc.setFontSize(8);
        doc.text(`Page ${doc.getNumberOfPages()}`, pageWidth - margin.right, 28, { align: "right" });
    };

    const resetPageCursor = () => {
        cursorY = margin.top;
        drawPageHeader();
    };

    const ensureSpace = (neededHeight) => {
        if (cursorY + neededHeight <= pageHeight - margin.bottom) return;
        doc.addPage();
        resetPageCursor();
    };

    const groups = [];
    const grouped = new Map();

    rows.forEach((row) => {
        const groupKey = metaColumns.length > 0
            ? metaColumns.map(col => stringifyExportValue(row?.[col.key])).join("||")
            : "__single__";
        let group = grouped.get(groupKey);
        if (!group) {
            group = {
                meta: Object.fromEntries(metaColumns.map(col => [col.key, row?.[col.key]])),
                items: [],
            };
            grouped.set(groupKey, group);
            groups.push(group);
        }
        group.items.push({
            field: stringifyExportValue(row?.field || "—") || "—",
            value: stringifyExportValue(row?.value || "—") || "—",
        });
    });

    resetPageCursor();

    groups.forEach((group, groupIdx) => {
        const headingParts = [
            stringifyExportValue(group.meta.messageType),
            stringifyExportValue(group.meta.reference || group.meta.messageReference),
        ].filter(Boolean);
        const metaPairs = metaColumns
            .map(col => [col.label, stringifyExportValue(group.meta[col.key] || "—")])
            .filter(([, value]) => value && value !== "—");
        const groupHeading = headingParts.join(" • ");
        const shouldShowHeading = !!groupHeading || groups.length > 1;

        if (shouldShowHeading) {
            ensureSpace(28);
            doc.setFont("helvetica", "bold");
            doc.setFontSize(10);
            doc.setTextColor(33, 56, 99);
            doc.text(groupHeading || `Record ${groupIdx + 1}`, margin.left, cursorY);
            cursorY += 10;
        }

        if (metaPairs.length > 0) {
            doc.autoTable({
                startY: cursorY,
                margin,
                tableWidth: contentWidth,
                theme: "grid",
                head: [["Field", "Value"]],
                body: metaPairs,
                styles: {
                    fontSize: 8,
                    cellPadding: { top: 4, right: 5, bottom: 4, left: 5 },
                    overflow: "linebreak",
                    valign: "top",
                    lineColor: [226, 232, 240],
                    lineWidth: 0.35,
                },
                headStyles: {
                    fillColor: [33, 56, 99],
                    textColor: [255, 255, 255],
                    fontStyle: "bold",
                    fontSize: 8,
                },
                columnStyles: {
                    0: { cellWidth: 126, fontStyle: "bold" },
                    1: { cellWidth: contentWidth - 126 },
                },
                didDrawPage: () => {
                    drawPageHeader();
                },
            });
            cursorY = (doc.lastAutoTable?.finalY || cursorY) + 12;
        }

        doc.autoTable({
            startY: cursorY,
            margin,
            tableWidth: contentWidth,
            theme: "grid",
            head: [["Field", "Value"]],
            body: group.items.map(item => [item.field, item.value]),
            styles: {
                fontSize: 8.5,
                cellPadding: { top: 5, right: 6, bottom: 5, left: 6 },
                overflow: "linebreak",
                valign: "top",
                lineColor: [226, 232, 240],
                lineWidth: 0.35,
            },
            headStyles: {
                fillColor: [33, 56, 99],
                textColor: [255, 255, 255],
                fontStyle: "bold",
                fontSize: 8.5,
            },
            columnStyles: {
                0: { cellWidth: 170, fontStyle: "bold", textColor: [55, 65, 81] },
                1: { cellWidth: contentWidth - 170, textColor: [17, 24, 39] },
            },
            didDrawPage: () => {
                drawPageHeader();
            },
        });
        cursorY = (doc.lastAutoTable?.finalY || cursorY) + 10;

        cursorY += 4;
        ensureSpace(12);
        doc.setDrawColor(203, 213, 225);
        doc.line(margin.left, cursorY, pageWidth - margin.right, cursorY);
        cursorY += 16;
    });

    doc.save(`${base}.pdf`);
};

const exportRowsAsFile = async ({ format, rows, columns, fileBaseName, title, sheetName, forceGenericPdf = false, showPdfTitle = true }) => {
    const safeRows = Array.isArray(rows) ? rows : [];
    const safeColumns = (columns && columns.length > 0)
        ? columns
        : [...new Set(safeRows.flatMap(r => Object.keys(r || {})))].map(k => ({ key: k, label: toPrettyLabel(k) }));
    const base = fileBaseName || "export";
    const exportTitle = title || base;

    if (format === "csv") {
        const header = safeColumns.map(c => escapeCsvCell(c.label)).join(",");
        const body = safeRows.map(row => safeColumns.map(c => escapeCsvCell(row?.[c.key])).join(",")).join("\n");
        triggerDownload(new Blob([`${header}\n${body}`], { type: "text/csv;charset=utf-8;" }), `${base}.csv`);
        return;
    }

    if (format === "json") {
        triggerDownload(new Blob([JSON.stringify(safeRows, null, 2)], { type: "application/json" }), `${base}.json`);
        return;
    }

    if (format === "xml") {
        const xmlEscape = (v) => stringifyExportValue(v)
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&apos;");
        const items = safeRows.map(row => {
            const fields = safeColumns.map(c => `<${c.key}>${xmlEscape(row?.[c.key])}</${c.key}>`).join("");
            return `<record>${fields}</record>`;
        }).join("");
        const xml = `<?xml version="1.0" encoding="UTF-8"?>\n<export title="${xmlEscape(exportTitle)}">${items}</export>`;
        triggerDownload(new Blob([xml], { type: "application/xml;charset=utf-8" }), `${base}.xml`);
        return;
    }

    if (format === "txt") {
        const lines = [safeColumns.map(c => c.label).join(" | ")];
        lines.push("-".repeat(Math.max(24, lines[0].length)));
        safeRows.forEach(row => lines.push(safeColumns.map(c => stringifyExportValue(row?.[c.key])).join(" | ")));
        triggerDownload(new Blob([lines.join("\n")], { type: "text/plain;charset=utf-8" }), `${base}.txt`);
        return;
    }

    if (format === "excel") {
        await loadScriptOnce("https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js", () => !!window.XLSX);
        const wsData = [safeColumns.map(c => c.label), ...safeRows.map(row => safeColumns.map(c => stringifyExportValue(row?.[c.key])))];
        const ws = window.XLSX.utils.aoa_to_sheet(wsData);
        ws["!cols"] = safeColumns.map(() => ({ wch: 24 }));
        const wb = window.XLSX.utils.book_new();
        window.XLSX.utils.book_append_sheet(wb, ws, sheetName || "Export");
        window.XLSX.writeFile(wb, `${base}.xlsx`);
        return;
    }

    if (format === "pdf") {
        if (!forceGenericPdf && safeColumns.some(c => c.key === "rawInput")) {
            await exportRawCopiesPdf({ base, exportTitle, rows: safeRows, columns: safeColumns, showPdfTitle });
            return;
        }

        if (!forceGenericPdf && safeColumns.some(c => c.key === "field") && safeColumns.some(c => c.key === "value")) {
            await exportFieldPairsPdf({ base, exportTitle, rows: safeRows, columns: safeColumns, showPdfTitle });
            return;
        }

        await loadScriptOnce("https://cdnjs.cloudflare.com/ajax/libs/jspdf/2.5.1/jspdf.umd.min.js", () => !!window.jspdf?.jsPDF);
        await loadScriptOnce("https://cdnjs.cloudflare.com/ajax/libs/jspdf-autotable/3.8.2/jspdf.plugin.autotable.min.js", () => !!window.jspdf?.jsPDF?.API?.autoTable);
        const { jsPDF } = window.jspdf;
        const columnWidths = estimatePdfColumnWidths(safeColumns, safeRows);
        const estimatedTableWidth = columnWidths.reduce((sum, width) => sum + width, 0);
        const landscape = safeColumns.length > 6 || estimatedTableWidth > 520;
        const pageFormat = safeColumns.length > 12 || estimatedTableWidth > 1000 ? "a3" : "a4";
        const repeatColumns = getPdfRepeatColumns(safeColumns);
        const margin = { top: 44, right: 24, bottom: 24, left: 24 };
        const fontSize = safeColumns.length > 14 ? 6 : safeColumns.length > 10 ? 7 : 8;
        const doc = new jsPDF({
            orientation: landscape ? "landscape" : "portrait",
            unit: "pt",
            format: pageFormat,
        });
        doc.autoTable({
            startY: margin.top,
            head: [safeColumns.map(c => c.label)],
            body: safeRows.map(row => safeColumns.map(c => stringifyExportValue(row?.[c.key]))),
            theme: "grid",
            tableWidth: "wrap",
            margin,
            styles: {
                fontSize,
                cellPadding: { top: 5, right: 4, bottom: 5, left: 4 },
                overflow: "linebreak",
                halign: "left",
                valign: "top",
                lineColor: [226, 232, 240],
                lineWidth: 0.35,
            },
            headStyles: {
                fillColor: [33, 56, 99],
                textColor: [255, 255, 255],
                fontStyle: "bold",
                fontSize,
                halign: "left",
                valign: "middle",
                overflow: "linebreak",
            },
            bodyStyles: {
                textColor: [17, 24, 39],
            },
            columnStyles: Object.fromEntries(
                columnWidths.map((width, index) => [index, { cellWidth: width, minCellWidth: Math.min(width, 72) }])
            ),
            horizontalPageBreak: estimatedTableWidth > 760,
            ...(repeatColumns.length
                ? { horizontalPageBreakRepeat: repeatColumns.length === 1 ? repeatColumns[0] : repeatColumns }
                : {}),
            didDrawPage: ({ pageNumber }) => {
                if (showPdfTitle) {
                    doc.setFontSize(12);
                    doc.setTextColor(17, 24, 39);
                    doc.text(exportTitle, margin.left, 28);
                }
                doc.setFontSize(8);
                doc.text(`Page ${pageNumber}`, doc.internal.pageSize.getWidth() - margin.right, 28, { align: "right" });
            },
        });
        doc.save(`${base}.pdf`);
        return;
    }

    if (format === "word") {
        const html = buildWordHtml(exportTitle, safeColumns, safeRows);
        triggerDownload(new Blob(["\ufeff", html], { type: "application/msword;charset=utf-8" }), `${base}.doc`);
        return;
    }

    throw new Error(`Unsupported export format: ${format}`);
};

// ── Table columns ─────────────────────────────────────────────────────────────
const COLUMNS = [
    { key: "sequenceNumber",      label: "Seq No.",         sortable: true },
    { key: "sessionNumber",       label: "Session No.",     sortable: true },
    { key: "logicalTerminalAddress", label: "Logical Terminal", sortable: true },
    { key: "format",              label: "Format",          sortable: true },
    { key: "type",                label: "Type",            sortable: true },
    { key: "date",                label: "Date",            sortable: true },
    { key: "time",                label: "Time",            sortable: true },
    { key: "direction",           label: "Direction",       sortable: true },
    { key: "network",             label: "Network",         sortable: true },
    { key: "networkStatus",       label: "Network Status",  sortable: true },
    { key: "deliveryMode",        label: "Delivery Mode",   sortable: true },
    { key: "service",             label: "Service",         sortable: true },
    { key: "sourceSystem",        label: "Source System",   sortable: true },
    { key: "sender",              label: "Sender",          sortable: true },
    { key: "receiver",            label: "Receiver",        sortable: true },
    { key: "correspondent",       label: "Correspondent",   sortable: true },
    { key: "status",              label: "Status",          sortable: true },
    { key: "currency",            label: "Currency",        sortable: true },
    { key: "amount",              label: "Amount",          sortable: true },
    { key: "userReference",       label: "User Ref",        sortable: true },
    { key: "uetr",                label: "UETR",            sortable: true },
    { key: "finCopy",             label: "FIN-COPY",        sortable: true },
    { key: "action",              label: "Action",          sortable: true },
    { key: "reason",              label: "Reason",          sortable: true },
    { key: "ownerUnit",           label: "Owner/Unit",      sortable: true },
    { key: "phase",               label: "Phase",           sortable: true },
    { key: "backendChannel",      label: "Channel",         sortable: true },
    { key: "processingType",      label: "Proc. Type",      sortable: true },
    { key: "profileCode",         label: "Profile",         sortable: true },
    { key: "amlStatus",           label: "AML Status",      sortable: true },
    { key: "amlDetails",          label: "AML Details",     sortable: true },
    { key: "nack",                label: "NACK",            sortable: true },
    { key: "messagePriority",     label: "Msg Priority",    sortable: true },
    { key: "possibleDuplicate",   label: "Dup?",            sortable: true },
    { key: "crossBorder",         label: "Cross Border",    sortable: true },
    { key: "workflowModel",       label: "Workflow Model",  sortable: true },
    { key: "originCountry",       label: "Origin Country",  sortable: true },
    { key: "destinationCountry",  label: "Dest. Country",   sortable: true },
    { key: "valueDate",           label: "Value Date",      sortable: true },
    { key: "settlementDate",      label: "Settlement Date", sortable: true },
];

const FIXED_TABLE_COLUMN_KEYS = new Set([
    "sequenceNumber",
    "sessionNumber",
    "logicalTerminalAddress",
    "format",
    "type",
    "date",
    "time",
    "direction",
    "network",
    "sourceSystem",
    "sender",
    "receiver",
    "correspondent",
    "status",
    "currency",
    "amount",
    "userReference",
    "uetr",
    "finCopy",
    "action",
    "reason",
    "ownerUnit",
    "phase",
    "backendChannel",
]);

const FIXED_TABLE_COLUMNS = COLUMNS.filter(col => FIXED_TABLE_COLUMN_KEYS.has(col.key));
const MAIN_EXPORT_TARGETS = [
    { key: "table", label: "Result Table" },
    { key: "header", label: "Header" },
    { key: "rawpayload", label: "Raw Payload" },
    { key: "payload", label: "Extended text" },
    { key: "history", label: "History" },
    { key: "details", label: "All Fields" },
];

const FIELD_DEFINITIONS = [
    { key: "format",               label: "Message Format",          group: "Classification", type: "select",       optKey: "formats",                placeholder: "All Formats",        stateKeys: ["format"],                                   colKeys: ["format"],             backendParam: "messageType"           },
    { key: "type",                 label: "Message Type",            group: "Classification", type: "select-type",  optKey: null,                     placeholder: "All Types",          stateKeys: ["type"],                                     colKeys: ["type"],               backendParam: "messageCode"           },
    { key: "direction",            label: "Message Direction",       group: "Classification", type: "select",       optKey: "directions",             placeholder: "All Directions",     stateKeys: ["direction"],                                colKeys: ["direction"],          backendParam: "io"                    },
    { key: "status",               label: "Status",                  group: "Classification", type: "select",       optKey: "statuses",               placeholder: "All Statuses",       stateKeys: ["status"],                                   colKeys: ["status"],             backendParam: "status"                },
    { key: "messagePriority",      label: "Message Priority",        group: "Classification", type: "select",       optKey: "messagePriorities",      placeholder: "All Priorities",     stateKeys: ["messagePriority"],                          colKeys: ["messagePriority"],    backendParam: "messagePriority"       },
    { key: "copyIndicator",        label: "Copy Indicator",          group: "Classification", type: "select",       optKey: "copyIndicators",         placeholder: "All",                stateKeys: ["copyIndicator"],                            colKeys: [],                     backendParam: "copyIndicator"         },
    { key: "finCopy",              label: "FIN-COPY",                group: "Classification", type: "select",       optKey: "finCopies",              placeholder: "All",                stateKeys: ["finCopy"],                                  colKeys: ["finCopy"],            backendParam: "finCopyService"        },
    { key: "possibleDuplicate",    label: "Possible Duplicate",      group: "Classification", type: "select",       optKey: null,                     placeholder: "All",                stateKeys: ["possibleDuplicate"],                        colKeys: [],                     backendParam: "possibleDuplicate",  options: ["true","false"] },
    { key: "dateRange",            label: "Creation Date Range",     group: "Date & Time",    type: "date-range",   optKey: null,                     placeholder: null,                 stateKeys: ["startDate","startTime","endDate","endTime"], colKeys: ["date","time"],         backendParam: "startDate,endDate"     },
    { key: "valueDateRange",       label: "Value Date Range",        group: "Date & Time",    type: "date-range2",  optKey: null,                     placeholder: null,                 stateKeys: ["valueDateFrom","valueDateTo"],               colKeys: ["valueDate"],          backendParam: "valueDateFrom,valueDateTo" },
    { key: "receivedDateRange",    label: "Received Date Range",     group: "Date & Time",    type: "date-range2",  optKey: null,                     placeholder: null,                 stateKeys: ["receivedDateFrom","receivedDateTo"],         colKeys: ["receivedDT"],         backendParam: "receivedDateFrom,receivedDateTo" },
    { key: "statusDateRange",      label: "Status Date Range",       group: "Date & Time",    type: "date-range2",  optKey: null,                     placeholder: null,                 stateKeys: ["statusDateFrom","statusDateTo"],             colKeys: ["statusDate"],         backendParam: "statusDateFrom,statusDateTo" },
    { key: "sender",               label: "Sender BIC",              group: "Parties",        type: "text",         placeholder: "Enter Sender BIC",                               stateKeys: ["sender"],                                   colKeys: ["sender"],             backendParam: "sender"                },
    { key: "receiver",             label: "Receiver BIC",            group: "Parties",        type: "text",         placeholder: "Enter Receiver BIC",                             stateKeys: ["receiver"],                                 colKeys: ["receiver"],           backendParam: "receiver"              },
    { key: "correspondent",        label: "Correspondent",           group: "Parties",        type: "text",         placeholder: "Enter Correspondent BIC",                        stateKeys: ["correspondent"],                            colKeys: ["correspondent"],      backendParam: "correspondent"         },
    { key: "mur",                  label: "User Reference (MUR)",    group: "References",     type: "text",         placeholder: "MUR",                                            stateKeys: ["userReference"],                            colKeys: ["userReference"],      backendParam: "mur"                   },
    { key: "sourceSystem",         label: "Source System",           group: "References",     type: "select",       optKey: "sourceSystems",          placeholder: "All Systems",        stateKeys: ["sourceSystem"],                             colKeys: ["sourceSystem"],       backendParam: "sourceSystem"          },
    { key: "rfkReference",         label: "RFK Reference / UMID",    group: "References",     type: "text",         placeholder: "Enter RFK Reference",                            stateKeys: ["rfkReference"],                            colKeys: [],                     backendParam: "relatedReference"      },
    { key: "messageReference",     label: "Message Reference",       group: "References",     type: "text",         placeholder: "Message Reference",                              stateKeys: ["messageReference"],                         colKeys: ["reference"],          backendParam: "reference"             },
    { key: "reference",            label: "Reference",               group: "References",     type: "text",         placeholder: "Reference",                                      stateKeys: ["reference"],                                colKeys: [],                     backendParam: "reference"             },
    { key: "transactionReference", label: "Transaction Reference",   group: "References",     type: "text",         placeholder: "Transaction Reference",                          stateKeys: ["transactionReference"],                     colKeys: [],                     backendParam: "transactionReference"  },
    { key: "sessionNumber",        label: "Session No.",             group: "References",     type: "text",         placeholder: "e.g. 0001",                                     stateKeys: ["sessionNumber"],                            colKeys: ["sessionNumber"],      backendParam: "sessionNumber"         },
    { key: "logicalTerminalAddress", label: "Logical Terminal",      group: "References",     type: "text",         placeholder: "e.g. BPXAINAAXPUN",                              stateKeys: ["logicalTerminalAddress"],                   colKeys: ["logicalTerminalAddress"], backendParam: "logicalTerminalAddress" },
    { key: "transferReference",    label: "Transfer Reference",      group: "References",     type: "text",         placeholder: "Transfer Reference",                             stateKeys: ["transferReference"],                        colKeys: [],                     backendParam: "transferReference"     },
    { key: "relatedReference",     label: "Related Reference",       group: "References",     type: "text",         placeholder: "Related Reference",                              stateKeys: ["relatedReference"],                         colKeys: [],                     backendParam: "relatedReference"      },
    { key: "uetr",                 label: "UETR",                    group: "References",     type: "text",         placeholder: "Enter UETR (e.g. 8a562c65-...)",                 stateKeys: ["uetr"],                                     colKeys: ["uetr"],               backendParam: "uetr"                  },
    { key: "mxInputReference",     label: "MX Input Reference",      group: "References",     type: "text",         placeholder: "MX Input Reference",                             stateKeys: ["mxInputReference"],                         colKeys: [],                     backendParam: "mxInputReference"      },
    { key: "mxOutputReference",    label: "MX Output Reference",     group: "References",     type: "text",         placeholder: "MX Output Reference",                            stateKeys: ["mxOutputReference"],                        colKeys: [],                     backendParam: "mxOutputReference"     },
    { key: "networkReference",     label: "Network Reference",       group: "References",     type: "text",         placeholder: "Network Reference",                              stateKeys: ["networkReference"],                         colKeys: [],                     backendParam: "networkReference"      },
    { key: "e2eMessageId",         label: "E2E Message ID",          group: "References",     type: "text",         placeholder: "End-to-End Message ID",                          stateKeys: ["e2eMessageId"],                             colKeys: [],                     backendParam: "e2eMessageId"          },
    { key: "seqRange",             label: "Sequence No. Range",      group: "References",     type: "seq-range",    placeholder: null,                                             stateKeys: ["seqFrom","seqTo"],                          colKeys: ["sequenceNumber"],     backendParam: "seqFrom,seqTo"         },
    { key: "amountRange",          label: "Amount Range",            group: "Financial",      type: "amount-range", placeholder: null,                                             stateKeys: ["amountFrom","amountTo"],                    colKeys: ["amount","currency"],  backendParam: "amountFrom,amountTo"   },
    { key: "currency",             label: "Currency (CCY)",          group: "Financial",      type: "select",       optKey: "currencies",             placeholder: "All Currencies",     stateKeys: ["currency"],                                 colKeys: ["currency"],           backendParam: "ccy"                   },
    { key: "network",              label: "Network Protocol",        group: "Routing",        type: "select",       optKey: "networks",               placeholder: "All Networks",       stateKeys: ["network"],                                  colKeys: ["network"],            backendParam: "networkProtocol"       },
    { key: "backendChannel",       label: "Channel / Session",       group: "Routing",        type: "select",       optKey: "backendChannels",        placeholder: "All Channels",       stateKeys: ["backendChannel"],                           colKeys: ["backendChannel"],     backendParam: "networkChannel"        },
    { key: "networkChannel",       label: "Network Channel",         group: "Routing",        type: "select",       optKey: "networkChannels",        placeholder: "All Channels",       stateKeys: ["networkChannel"],                           colKeys: ["backendChannel"],     backendParam: "networkChannel"        },
    { key: "networkPriority",      label: "Network Priority",        group: "Routing",        type: "select",       optKey: "networkPriorities",      placeholder: "All Priorities",     stateKeys: ["networkPriority"],                          colKeys: [],                     backendParam: "networkPriority"       },
    { key: "deliveryMode",         label: "Delivery Mode",           group: "Routing",        type: "select",       optKey: "deliveryModes",          placeholder: "All Modes",          stateKeys: ["deliveryMode"],                             colKeys: ["deliveryMode"],       backendParam: "deliveryMode"          },
    { key: "service",              label: "Service",                 group: "Routing",        type: "select",       optKey: "services",               placeholder: "All Services",       stateKeys: ["service"],                                  colKeys: ["service"],            backendParam: "service"               },
    { key: "country",              label: "Country",                 group: "Routing",        type: "select",       optKey: "countries",              placeholder: "All Countries",      stateKeys: ["country"],                                  colKeys: [],                     backendParam: "country"               },
    { key: "originCountry",        label: "Origin Country",          group: "Routing",        type: "select",       optKey: "originCountries",        placeholder: "All Countries",      stateKeys: ["originCountry"],                            colKeys: [],                     backendParam: "originCountry"         },
    { key: "destinationCountry",   label: "Destination Country",     group: "Routing",        type: "select",       optKey: "destinationCountries",   placeholder: "All Countries",      stateKeys: ["destinationCountry"],                       colKeys: [],                     backendParam: "destinationCountry"    },
    { key: "ownerUnit",            label: "Owner / Unit",            group: "Ownership",      type: "select",       optKey: "ownerUnits",             placeholder: "All Units",          stateKeys: ["ownerUnit"],                                colKeys: ["ownerUnit"],          backendParam: "owner"                 },
    { key: "workflow",             label: "Workflow",                group: "Ownership",      type: "select",       optKey: "workflows",              placeholder: "All Workflows",      stateKeys: ["workflow"],                                 colKeys: [],                     backendParam: "workflow"              },
    { key: "workflowModel",        label: "Workflow Model",          group: "Ownership",      type: "select",       optKey: "workflowModels",         placeholder: "All Models",         stateKeys: ["workflowModel"],                            colKeys: [],                     backendParam: "workflowModel"         },
    { key: "originatorApplication",label: "Originator Application",  group: "Ownership",      type: "select",       optKey: "originatorApplications", placeholder: "All Applications",   stateKeys: ["originatorApplication"],                    colKeys: [],                     backendParam: "originatorApplication" },
    { key: "phase",                label: "Phase",                   group: "Lifecycle",      type: "select",       optKey: "phases",                 placeholder: "All Phases",         stateKeys: ["phase"],                                    colKeys: ["phase"],              backendParam: "phase"                 },
    { key: "action",               label: "Action",                  group: "Lifecycle",      type: "select",       optKey: "actions",                placeholder: "All Actions",        stateKeys: ["action"],                                   colKeys: ["action"],             backendParam: "action"                },
    { key: "reason",               label: "Reason",                  group: "Lifecycle",      type: "text",         placeholder: "Enter Reason",                                  stateKeys: ["reason"],                                   colKeys: ["reason"],             backendParam: "reason"                },
    { key: "processingType",       label: "Processing Type",         group: "Processing",     type: "select",       optKey: "processingTypes",        placeholder: "All Types",          stateKeys: ["processingType"],                           colKeys: ["processingType"],     backendParam: "processingType"        },
    { key: "processPriority",      label: "Process Priority",        group: "Processing",     type: "select",       optKey: "processPriorities",      placeholder: "All Priorities",     stateKeys: ["processPriority"],                          colKeys: [],                     backendParam: "processPriority"       },
    { key: "profileCode",          label: "Profile Code",            group: "Processing",     type: "select",       optKey: "profileCodes",           placeholder: "All Profiles",       stateKeys: ["profileCode"],                              colKeys: [],                     backendParam: "profileCode"           },
    { key: "environment",          label: "Environment",             group: "Processing",     type: "select",       optKey: "environments",           placeholder: "All Environments",   stateKeys: ["environment"],                              colKeys: [],                     backendParam: "environment"           },
    { key: "nack",                 label: "NACK Code",               group: "Processing",     type: "select",       optKey: "nackCodes",              placeholder: "All",                stateKeys: ["nack"],                                     colKeys: ["nack"],               backendParam: "nack"                  },
    { key: "amlStatus",            label: "AML Status",              group: "Compliance",     type: "select",       optKey: "amlStatuses",            placeholder: "All Statuses",       stateKeys: ["amlStatus"],                                colKeys: ["amlStatus"],          backendParam: "amlStatus"             },
    { key: "amlDetails",           label: "AML Details",             group: "Compliance",     type: "text",         placeholder: "AML reference...",                               stateKeys: ["amlDetails"],                               colKeys: ["amlDetails"],         backendParam: "amlDetails"            },
    { key: "historyEntity",        label: "History Entity",          group: "History",        type: "text",         placeholder: "e.g. Screening, Validation",                     stateKeys: ["historyEntity"],                            colKeys: [],                     backendParam: "historyEntity"         },
    { key: "historyDescription",   label: "History Comment",         group: "History",        type: "text",         placeholder: "Search history comments",                        stateKeys: ["historyDescription"],                       colKeys: [],                     backendParam: "historyDescription"    },
    { key: "historyPhase",         label: "History Phase",           group: "History",        type: "select",       optKey: "phases",         placeholder: "All Phases",          stateKeys: ["historyPhase"],                             colKeys: [],                     backendParam: "historyPhase"          },
    { key: "historyAction",        label: "History Action",          group: "History",        type: "select",       optKey: "actions",        placeholder: "All Actions",         stateKeys: ["historyAction"],                            colKeys: [],                     backendParam: "historyAction"         },
    { key: "historyUser",          label: "History User",            group: "History",        type: "text",         placeholder: "e.g. SYS_USER_01",                               stateKeys: ["historyUser"],                              colKeys: [],                     backendParam: "historyUser"           },
    { key: "historyChannel",       label: "History Channel",         group: "History",        type: "text",         placeholder: "e.g. ADCBGBS0",                                  stateKeys: ["historyChannel"],                           colKeys: [],                     backendParam: "historyChannel"        },
    { key: "block4Value",          label: "Payload Field Value",     group: "Payload",        type: "text-wide",    placeholder: "Search in raw FIN fields",                       stateKeys: ["block4Value"],                              colKeys: [],                     backendParam: "block4Value"           },
    { key: "freeSearchText",       label: "Free Search Text",        group: "Other",          type: "text-wide",    placeholder: "Searches across all fields...",                  stateKeys: ["freeSearchText"],                           colKeys: [],                     backendParam: "freeSearchText"        },
];

const FIELD_KEY_ALIASES = {
    amount: "amountRange",
    ccy: "currency",
    creationDate: "dateRange",
    finCopyService: "finCopy",
    io: "direction",
    messageCode: "type",
    messageType: "format",
    networkProtocol: "network",
    owner: "ownerUnit",
    receivedDate: "receivedDateRange",
    sequenceNumber: "seqRange",
    statusDate: "statusDateRange",
    valueDate: "valueDateRange",
};

const STATIC_COLUMN_KEY_SET = new Set(COLUMNS.map(col => col.key));
const STATIC_FIELD_DEF_MAP = new Map(FIELD_DEFINITIONS.map(def => [def.key, def]));

const resolveFieldKey = (key) => FIELD_KEY_ALIASES[key] || key;

const buildAdvancedDerivedColumns = (selectedDefs, rows = []) => {
    const derived = [];
    const seen = new Set();
    selectedDefs.forEach(def => {
        const explicitColKeys = def.colKeys || [];
        if (explicitColKeys.length > 0) return;

        const candidateKeys = [
            ...(def.stateKeys || []),
            def.key,
            def.backendParam,
        ].filter(Boolean);

        const displayKey = candidateKeys.find(key =>
            !seen.has(key) &&
            key !== "reference" &&
            key !== "messageReference" &&
            rows.some(row => row && row[key] !== undefined && row[key] !== null && row[key] !== "")
        );

        if (!displayKey) return;
        seen.add(displayKey);
        derived.push({
            key: displayKey,
            label: def.columnLabel || def.label,
            sortable: true,
            isDynamic: true,
        });
    });
    return derived;
};

const normalizeDynamicField = (field) => {
    const resolvedKey = resolveFieldKey(field.key);
    const baseDef = STATIC_FIELD_DEF_MAP.get(resolvedKey);

    if (baseDef) {
        return {
            ...baseDef,
            key: resolvedKey,
            label: baseDef.label,
            group: baseDef.group,
            type: baseDef.type,
            optKey: baseDef.optKey ?? null,
            _backendOpts: field.options || [],
            placeholder: baseDef.placeholder || (field.options?.length ? `All ${baseDef.label}` : `Enter ${baseDef.label}`),
            stateKeys: [...(baseDef.stateKeys || [resolvedKey])],
            colKeys: [...(baseDef.colKeys || [])],
            backendParam: baseDef.backendParam || field.backendParam,
            columnLabel: field.columnLabel || baseDef.columnLabel || baseDef.label,
            showInTable: field.showInTable || (baseDef.colKeys || []).length > 0,
        };
    }

    return {
        key: resolvedKey,
        label: field.label,
        group: field.group,
        type: field.type,
        optKey: null,
        _backendOpts: field.options || [],
        placeholder: field.options?.length ? `All ${field.label}` : `Enter ${field.label}`,
        stateKeys: [resolvedKey],
        colKeys: field.showInTable ? [resolvedKey] : [],
        backendParam: field.backendParam,
        columnLabel: field.columnLabel,
        showInTable: field.showInTable,
    };
};

const getDynamicExtraColumns = (fields) => {
    const seen = new Set();
    const extraCols = [];

    fields.forEach(field => {
        (field.colKeys || []).forEach(colKey => {
            if (!colKey || STATIC_COLUMN_KEY_SET.has(colKey) || seen.has(colKey)) return;
            seen.add(colKey);
            extraCols.push({
                key: colKey,
                label: field.columnLabel || field.label || toPrettyLabel(colKey),
                sortable: true,
                isDynamic: true,
            });
        });
    });

    return extraCols;
};

const FIELD_GROUPS = ["Classification", "Date & Time", "Parties", "References", "Financial", "Routing", "Ownership", "Lifecycle", "Processing", "Compliance", "History", "Other"];
const ADV_HIDDEN_GROUPS = new Set(["Payload"]);
const ADV_PICKER_GROUP_ORDER = ["Dropdown Search", "Text Search", "Range & Date Search", "Other Search"];
const ADV_FIXED_ALLOWED_FIELD_KEYS = new Set([
    "format", "type", "dateRange", "mur", "sourceSystem", "rfkReference",
    "direction", "status", "finCopy", "network", "sender", "receiver",
    "phase", "action", "reason", "correspondent", "amountRange", "currency",
    "ownerUnit", "messageReference", "seqRange", "sessionNumber",
    "logicalTerminalAddress", "uetr", "freeSearchText", "backendChannel", "networkChannel"
]);
const ADV_FIXED_ALLOWED_STATE_KEYS = new Set([
    "format", "type", "startDate", "startTime", "endDate", "endTime",
    "userReference", "sourceSystem", "rfkReference", "direction", "status",
    "finCopy", "network", "sender", "receiver", "phase", "action", "reason",
    "correspondent", "amountFrom", "amountTo", "currency", "ownerUnit",
    "messageReference", "seqFrom", "seqTo", "sessionNumber",
    "logicalTerminalAddress", "uetr", "freeSearchText", "backendChannel", "networkChannel"
]);
const ADV_BASE_COLS = new Set(["sequenceNumber", "sessionNumber", "format", "type", "date", "time"]);
const SORT_NONE = null, SORT_ASC = "asc", SORT_DESC = "desc";
const MONTHS = ["January","February","March","April","May","June","July","August","September","October","November","December"];
const DAYS   = ["Su","Mo","Tu","We","Th","Fr","Sa"];

const getAdvancedPickerGroup = (field) => {
    if (!field) return "Other Search";
    if (field.type === "select" || field.type === "select-type") return "Dropdown Search";
    if (field.type === "text" || field.type === "text-wide") return "Text Search";
    if (["date-range", "value-date-range", "received-date-range", "status-date-range", "seq-range", "amount-range"].includes(field.type)) {
        return "Range & Date Search";
    }
    return "Other Search";
};

const isAdvancedFixedField = (field) => {
    if (!field) return false;
    if (ADV_FIXED_ALLOWED_FIELD_KEYS.has(field.key)) return true;
    const stateKeys = Array.isArray(field.stateKeys) ? field.stateKeys : [];
    return stateKeys.some(key => ADV_FIXED_ALLOWED_STATE_KEYS.has(key));
};

const initialSearchState = {
    format:"", type:"", messageCode:"", startDate:"", startTime:"", endDate:"", endTime:"",
    direction:"", network:"", sender:"", receiver:"",
    status:"", currency:"", userReference:"", rfkReference:"",
    messageReference:"", uetr:"", finCopy:"", action:"", reason:"",
    correspondent:"", amountFrom:"", amountTo:"", seqFrom:"",
    seqTo:"", sessionNumber:"", ownerUnit:"", freeSearchText:"", backendChannel:"", phase:"",
    country:"", workflow:"", networkChannel:"", networkPriority:"",
    reference:"", transactionReference:"", transferReference:"",
    historyEntity:"", historyDescription:"", historyPhase:"", historyAction:"", historyUser:"", historyChannel:"",
    block4Value:"",
    relatedReference:"", mxInputReference:"", mxOutputReference:"",
    networkReference:"", e2eMessageId:"",
    networkStatus:"", deliveryMode:"", service:"",
    originCountry:"", destinationCountry:"",
    workflowModel:"", originatorApplication:"",
    processingType:"", processPriority:"", profileCode:"", environment:"", nack:"",
    amlStatus:"", amlDetails:"",
    messagePriority:"", copyIndicator:"", possibleDuplicate:"", crossBorder:"",
    logicalTerminalAddress:"",
    valueDateFrom:"", valueDateTo:"",
    receivedDateFrom:"", receivedDateTo:"",
    statusDateFrom:"", statusDateTo:"",
};

const initialRcFilters = {
    messageReference: "", sender: "", receiver: "",
    messageTypeCode: "", direction: "", currentStatus: "", protocol: "",
    inputType: "", source: "", startDate: "", endDate: "", freeText: "",
};

const emptyOpts = {
    formats:[], types:[], mtTypes:[], mxTypes:[], allMtMxTypes:[],
    networks:[], sourceSystems:[], currencies:[], ownerUnits:[],
    backendChannels:[], directions:[], statuses:[], finCopies:[], actions:[], phases:[],
    messageCodes:[], senders:[], receivers:[], countries:[],
    workflows:[], networkChannels:[], networkPriorities:[], ioDirections:[],
    networkStatuses:[], deliveryModes:[], services:[],
    originCountries:[], destinationCountries:[],
    workflowModels:[], originatorApplications:[],
    processingTypes:[], processPriorities:[], profileCodes:[], environments:[],
    amlStatuses:[], nackCodes:[], messagePriorities:[], copyIndicators:[],
    finCopyServices:[], reasons:[],
};


// ── DateTimePicker ─────────────────────────────────────────────────────────────
function DateTimePicker({ label, dateValue, timeValue, onDateChange, onTimeChange, onKeyDown }) {
    const [open, setOpen] = useState(false);
    const [viewYear, setViewYear] = useState(() => dateValue ? parseInt(dateValue.split("/")[0]) || new Date().getFullYear() : new Date().getFullYear());
    const [viewMonth, setViewMonth] = useState(() => dateValue ? (parseInt(dateValue.split("/")[1]) - 1) || new Date().getMonth() : new Date().getMonth());
    const [timeMode, setTimeMode] = useState(false);
    const [typedDate, setTypedDate] = useState(dateValue || "");
    const [dateError, setDateError] = useState(false);
    const ref = useRef(null);

    const isValidDate = useCallback((str) => {
        if (!str) return true;
        const m = str.match(/^(\d{4})\/(\d{2})\/(\d{2})$/);
        if (!m) return false;
        const [, y, mo, d] = m.map(Number);
        if (mo < 1 || mo > 12) return false;
        return d >= 1 && d <= new Date(y, mo, 0).getDate();
    }, []);

    const commitDate = useCallback((v) => {
        if (!v) { onDateChange(""); setDateError(false); return; }
        if (isValidDate(v)) { onDateChange(v); setDateError(false); }
        else setDateError(true);
    }, [onDateChange, isValidDate]);

    useEffect(() => { setTypedDate(dateValue || ""); }, [dateValue]);
    useEffect(() => {
        const handle = (e) => { if (ref.current && !ref.current.contains(e.target)) { setOpen(false); setTimeMode(false); commitDate(typedDate); } };
        document.addEventListener("mousedown", handle);
        return () => document.removeEventListener("mousedown", handle);
    }, [typedDate, commitDate]);
    useEffect(() => {
        if (dateValue) {
            const p = dateValue.split("/");
            if (p[0] && parseInt(p[0]) > 999) setViewYear(parseInt(p[0]));
            if (p[1]) setViewMonth(parseInt(p[1]) - 1);
        }
    }, [dateValue]);

    const handleDateTyping = (raw) => {
        let v = raw.replace(/[^\d/]/g, "");
        if (v.length === 4 && !v.includes("/")) v += "/";
        else if (v.length === 7 && v.split("/").length === 2) v += "/";
        if (v.length > 10) v = v.slice(0, 10);
        setTypedDate(v); setDateError(false);
        if (v.length === 10) commitDate(v);
    };
    const handleDateKey = (e) => {
        if (e.key === "Enter") { commitDate(typedDate); if (onKeyDown) onKeyDown(e); }
        if (e.key === "Tab") commitDate(typedDate);
    };
    const handleClearAll = (e) => { e.stopPropagation(); setTypedDate(""); setDateError(false); onDateChange(""); onTimeChange(""); };

    const getDaysInMonth = (y, m) => new Date(y, m + 1, 0).getDate();
    const getFirstDay    = (y, m) => new Date(y, m, 1).getDay();
    const selectedDay    = dateValue ? parseInt(dateValue.split("/")[2]) : null;
    const selectedMonth  = dateValue ? parseInt(dateValue.split("/")[1]) - 1 : null;
    const selectedYear   = dateValue ? parseInt(dateValue.split("/")[0]) : null;

    const handleDayClick = (day) => {
        const d = String(day).padStart(2,"0"), mo = String(viewMonth+1).padStart(2,"0");
        const newDate = `${viewYear}/${mo}/${d}`;
        setTypedDate(newDate); onDateChange(newDate); setDateError(false); setTimeMode(true);
    };
    const prevMonth = () => { if (viewMonth===0){setViewMonth(11);setViewYear(y=>y-1);}else setViewMonth(m=>m-1); };
    const nextMonth = () => { if (viewMonth===11){setViewMonth(0);setViewYear(y=>y+1);}else setViewMonth(m=>m+1); };

    const daysInMonth = getDaysInMonth(viewYear, viewMonth);
    const firstDay    = getFirstDay(viewYear, viewMonth);
    const today       = new Date();
    const todayY = today.getFullYear(), todayM = today.getMonth(), todayD = today.getDate();

    const [hh, mm, ss] = (timeValue || "").split(":");
    const setHH = (v) => onTimeChange(`${String(v).padStart(2,"0")}:${mm||"00"}:${ss||"00"}`);
    const setMM = (v) => onTimeChange(`${hh||"00"}:${String(v).padStart(2,"0")}:${ss||"00"}`);
    const setSS = (v) => onTimeChange(`${hh||"00"}:${mm||"00"}:${String(v).padStart(2,"0")}`);

    return (
        <div className="dtp-wrap" ref={ref}>
            {label && <label>{label}</label>}
            <div className={`dtp-input-row${open?" dtp-row-open":""}`}>
                <div className={`dtp-segment${dateError?" dtp-segment-error":""}`}>
                    <svg className="dtp-seg-icon" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round"><rect x="3" y="4" width="18" height="18" rx="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/></svg>
                    <input className="dtp-type-input" placeholder="YYYY/MM/DD" value={typedDate} maxLength={10} onChange={e=>handleDateTyping(e.target.value)} onKeyDown={handleDateKey} onBlur={()=>commitDate(typedDate)} autoComplete="off" spellCheck={false}/>
                    {dateError && <span className="dtp-error-dot" title="Invalid date"/>}
                </div>
                {timeValue && (<><span className="dtp-seg-sep">·</span><div className="dtp-time-badge" onClick={()=>{setOpen(true);setTimeMode(true);}}><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg><span>{timeValue}</span></div></>)}
                <div className="dtp-seg-actions">
                    {(dateValue||timeValue) && <button className="dtp-clear-btn" onClick={handleClearAll} tabIndex={-1}><svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg></button>}
                    <button className={`dtp-cal-toggle${open?" dtp-cal-toggle-active":""}`} onClick={()=>setOpen(p=>!p)} tabIndex={-1}><svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round"><rect x="3" y="4" width="18" height="18" rx="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/></svg></button>
                </div>
            </div>
            {open && (
                <div className="dtp-dropdown">
                    <div className="dtp-tab-row">
                        <button className={`dtp-tab${!timeMode?" dtp-tab-active":""}`} onClick={()=>setTimeMode(false)}><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><rect x="3" y="4" width="18" height="18" rx="2"/><line x1="3" y1="10" x2="21" y2="10"/></svg>Date</button>
                        <button className={`dtp-tab${timeMode?" dtp-tab-active":""}`} onClick={()=>setTimeMode(true)}><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>Time</button>
                    </div>
                    {!timeMode ? (
                        <div className="dtp-calendar">
                            <div className="dtp-cal-nav"><button className="dtp-nav-btn" onClick={prevMonth}>‹</button><span className="dtp-cal-title">{MONTHS[viewMonth]} {viewYear}</span><button className="dtp-nav-btn" onClick={nextMonth}>›</button></div>
                            <div className="dtp-year-row"><button className="dtp-year-step" onClick={()=>setViewYear(y=>y-1)}>«</button><span className="dtp-year-val">{viewYear}</span><button className="dtp-year-step" onClick={()=>setViewYear(y=>y+1)}>»</button></div>
                            <div className="dtp-day-grid">
                                {DAYS.map(d=><span key={d} className="dtp-day-hdr">{d}</span>)}
                                {Array.from({length:firstDay}).map((_,i)=><span key={`e${i}`}/>)}
                                {Array.from({length:daysInMonth}).map((_,i)=>{
                                    const day=i+1;
                                    const isSel=day===selectedDay&&viewMonth===selectedMonth&&viewYear===selectedYear;
                                    const isToday=day===todayD&&viewMonth===todayM&&viewYear===todayY;
                                    return <button key={day} className={`dtp-day${isSel?" dtp-day-selected":""}${isToday&&!isSel?" dtp-day-today":""}`} onClick={()=>handleDayClick(day)}>{day}</button>;
                                })}
                            </div>
                            <div className="dtp-cal-footer">
                                <button className="dtp-today-btn" onClick={()=>{setViewYear(todayY);setViewMonth(todayM);handleDayClick(todayD);}}>Today</button>
                                {dateValue&&<button className="dtp-time-btn" onClick={()=>setTimeMode(true)}>Set Time →</button>}
                            </div>
                        </div>
                    ) : (
                        <div className="dtp-time-panel">
                            <div className="dtp-time-header"><svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>Set Time <span className="dtp-time-optional">(optional)</span></div>
                            <div className="dtp-time-cols">
                                {[["HH",hh,0,23,setHH],["MM",mm,0,59,setMM],["SS",ss,0,59,setSS]].map(([lbl,val,min,max,setter],idx)=>(
                                    <React.Fragment key={lbl}>
                                        {idx>0&&<span className="dtp-time-colon">:</span>}
                                        <div className="dtp-time-col">
                                            <span className="dtp-time-lbl">{lbl}</span>
                                            <button className="dtp-spin-btn" onClick={()=>setter(Math.min(max,parseInt(val||0)+1))}>▲</button>
                                            <input className="dtp-time-input" type="number" min={min} max={max} value={val||""} placeholder="00" onChange={e=>setter(Math.max(min,Math.min(max,parseInt(e.target.value)||0)))}/>
                                            <button className="dtp-spin-btn" onClick={()=>setter(Math.max(min,parseInt(val||0)-1))}>▼</button>
                                        </div>
                                    </React.Fragment>
                                ))}
                            </div>
                            <div className="dtp-time-presets">
                                {[["Start of Day","00:00:00"],["End of Day","23:59:59"],["Noon","12:00:00"]].map(([lbl,val])=>(
                                    <button key={lbl} className="dtp-preset-btn" onClick={()=>onTimeChange(val)}>{lbl}</button>
                                ))}
                            </div>
                            <div className="dtp-time-footer">
                                <button className="dtp-back-btn" onClick={()=>setTimeMode(false)}>← Back to Calendar</button>
                                <button className="dtp-done-btn" onClick={()=>{setOpen(false);setTimeMode(false);}}>Done</button>
                            </div>
                        </div>
                    )}
                </div>
            )}
        </div>
    );
}


// ── Dynamic Select ─────────────────────────────────────────────────────────────
function DynSelect({ value, onChange, placeholder, options, loading }) {
    return (
        <select value={value} onChange={onChange} disabled={loading}>
            <option value="">{loading ? "Loading..." : placeholder}</option>
            {options.map(o => <option key={o} value={o}>{o}</option>)}
        </select>
    );
}


function FloatingModal({
    modal,
    processed,
    onClose,
    onBringToFront,
    onPatch,
    onPrev,
    onNext,
    getDisplayFormat,
    getDisplayType,
    statusCls,
    dirClass,
    formatDirection,
    token,
    onNotify,
}) {
    const boxRef   = useRef(null);
    const dragRef  = useRef(null);
    const resRef   = useRef(null);
    const livePos  = useRef({ x: modal.pos.x,  y: modal.pos.y  });
    const liveSize = useRef({ w: modal.size.w, h: modal.size.h });
    const modalExportRef = useRef(null);

    const [modalExportOpen, setModalExportOpen] = useState(false);
    const [modalExportTargets, setModalExportTargets] = useState([tab]);
    const [modalExporting, setModalExporting] = useState(false);

    // ── Raw Copies for this message ───────────────────────────────────────
    const [modalRcData,    setModalRcData]    = useState(null);
    const [modalRcLoading, setModalRcLoading] = useState(false);
    const [modalRcError,   setModalRcError]   = useState(null);
    const [modalRcExpanded,setModalRcExpanded]= useState(null);
    const [modalRcCopied,  setModalRcCopied]  = useState(null);
    const [detailLoading,  setDetailLoading]  = useState(false);

    const { id, msg, tab, pos, size, zIndex, index, _flash } = modal;
    const notify = useCallback((text, type = "info") => {
        if (typeof onNotify === "function") onNotify(text, type);
    }, [onNotify]);
    const safeFileNamePart = useCallback((v) => String(v || "").replace(/[\\/:*?"<>|]+/g, "_").slice(0, 70), []);
    const modalRefValue = msg.reference || msg.messageReference || msg.rawMessage?.messageReference || null;
    const modalMessageKey = [
        index,
        msg.id,
        msg.sequenceNumber,
        msg.reference,
        msg.transactionReference,
        msg.messageReference,
        msg.date,
        msg.time,
    ].map(v => String(v ?? "")).join("|");
    const modalBaseName = `swift_popup_${safeFileNamePart(modalRefValue || msg.sequenceNumber || "message")}`;
    const hasModalDetailData = Boolean(
        msg.rawMessage ||
        (Array.isArray(msg.block4Fields) && msg.block4Fields.length) ||
        (Array.isArray(msg.historyLines) && msg.historyLines.length) ||
        (msg.rawFin && msg.rawFin !== "—")
    );
    const tabNeedsDetailData = ["rawpayload", "payload", "history", "details"].includes(tab);
    const showDetailLoading = detailLoading && tabNeedsDetailData && !hasModalDetailData;

    // Flash animation when duplicate window is focused
    useEffect(() => {
        if (!_flash || !boxRef.current) return;
        const el = boxRef.current;
        el.style.transition = "box-shadow 0.08s ease-in-out";
        el.style.boxShadow  = "0 0 0 3px var(--accent), 0 20px 60px rgba(0,0,0,0.35)";
        const t = setTimeout(() => {
            el.style.boxShadow = "";
            el.style.transition = "";
        }, 500);
        return () => clearTimeout(t);
    }, [_flash]);
    const isFirst = index <= 0;
    const isLast  = index >= processed.length - 1;

    const fetchModalRawCopies = useCallback(async () => {
        if (!modalRefValue) return null;
        const r = await fetch(`${API_RAW_COPIES_URL}/by-ref/${encodeURIComponent(modalRefValue)}`, {
            headers: { "Content-Type": "application/json", ...(token ? { Authorization: `Bearer ${token}` } : {}) }
        });
        if (!r.ok) throw new Error(`Raw Copies fetch failed (${r.status})`);
        const d = await r.json();
        return d.data || d;
    }, [modalRefValue, token]);

    useEffect(() => {
        if (tab !== "rawcopies") return;
        if (!modalRefValue) {
            setModalRcData(null);
            setModalRcError(null);
            setModalRcLoading(false);
            setModalRcExpanded(null);
            setModalRcCopied(null);
            return;
        }
        let alive = true;
        setModalRcLoading(true);
        setModalRcError(null);
        setModalRcData(null);
        setModalRcExpanded(null);
        setModalRcCopied(null);
        fetchModalRawCopies()
            .then((payload) => {
                if (!alive) return;
                setModalRcData(payload);
                setModalRcLoading(false);
            })
            .catch(e => {
                if (!alive) return;
                setModalRcError(e.message);
                setModalRcLoading(false);
            });
        return () => { alive = false; };
    }, [tab, modalMessageKey, modalRefValue, fetchModalRawCopies]);

    useEffect(() => {
        if (!modalExportOpen) return;
        const handle = (e) => {
            if (modalExportRef.current && !modalExportRef.current.contains(e.target)) setModalExportOpen(false);
        };
        document.addEventListener("mousedown", handle);
        return () => document.removeEventListener("mousedown", handle);
    }, [modalExportOpen]);

    const modalTabs = [
        {
            key: "rawpayload",
            label: "Raw Payload",
        },
        {
            key: "payload",
            label: "Extended text",
            count: (msg.block4Fields || msg.rawMessage?.mtPayload?.block4Fields || []).length,
        },
        {
            key: "history",
            label: "History",
            count: (msg.historyLines || msg.rawMessage?.historyLines || []).length,
        },
        {
            key: "rawcopies",
            label: "Raw Copies",
            count: modalRcData?.copies?.length || 0,
        },
        {
            key: "details",
            label: "All Fields",
        },
    ];

    const orderedModalExportTargets = modalExportTargets.length ? modalExportTargets : [tab];
    const resolvedModalExportTarget = orderedModalExportTargets.length === 1 ? orderedModalExportTargets[0] : "";
    const modalExportFormats = useMemo(() => getExportFormatOptions({
        targetKey: resolvedModalExportTarget,
        includeMtOnly: orderedModalExportTargets.length === 1 && resolvedModalExportTarget === "rawpayload" && isMtMessage(msg),
        selectedTargets: orderedModalExportTargets,
    }), [resolvedModalExportTarget, orderedModalExportTargets, msg]);

    const toggleModalExportTarget = useCallback((targetKey) => {
        setModalExportTargets((prev) => {
            if (prev.includes(targetKey)) return prev.length === 1 ? prev : prev.filter(key => key !== targetKey);
            return [...prev, targetKey];
        });
    }, []);

    useEffect(() => {
        if (tab === "body" || tab === "header") onPatch(id, { tab: "history" });
    }, [tab, id, onPatch]);

    useEffect(() => {
        if (!modalRefValue || hasModalDetailData) {
            setDetailLoading(false);
            return;
        }
        let alive = true;
        setDetailLoading(true);
        fetch(`${API_DETAIL_BY_REF_URL}/${encodeURIComponent(modalRefValue)}`, {
            headers: { "Content-Type": "application/json", ...(token ? { Authorization: `Bearer ${token}` } : {}) }
        })
            .then(r => {
                if (!r.ok) throw new Error(`Detail fetch failed (${r.status})`);
                return r.json();
            })
            .then(detail => {
                if (!alive || !detail) return;
                onPatch(id, { msg: detail });
            })
            .catch(() => {})
            .finally(() => {
                if (alive) setDetailLoading(false);
            });
        return () => { alive = false; };
    }, [id, modalRefValue, modalMessageKey, hasModalDetailData, onPatch, token]);

    const getSectionData = useCallback((sectionKey, rcDataOverride = modalRcData) => {
        const raw = msg.rawMessage || {};
        if (sectionKey === "header") {
            const pairs = getHeaderExportPairs(msg);
            return {
                label: "Header",
                columns: [{ key: "field", label: "Field" }, { key: "value", label: "Value" }],
                rows: pairs.map(item => ({ field: item.label, value: stringifyExportValue(item.val) })),
            };
        }

        if (sectionKey === "body") {
            const pairs = [
                ["Message Code", msg.messageCode || getDisplayType(msg)],
                ["Message Type", getDisplayFormat(msg)],
                ["Network Protocol", msg.networkProtocol || msg.network],
                ["Network Channel", msg.networkChannel || msg.backendChannel],
                ["Network Priority", msg.networkPriority],
                ["Country", msg.country],
                ["Owner", msg.owner || msg.ownerUnit],
                ["Workflow", msg.workflow],
                ["Direction", formatDirection(msg.io || msg.direction)],
                ["Status", msg.status],
                ["Phase", msg.phase],
                ["Action", msg.action],
                ["Reason", msg.reason],
                ["Environment", msg.environment],
                ["Session No.", msg.sessionNumber],
                ["Sequence No.", msg.sequenceNumber],
            ];
            return {
                label: "Body",
                columns: [{ key: "field", label: "Field" }, { key: "value", label: "Value" }],
                rows: pairs.map(([field, value]) => ({ field, value: stringifyExportValue(value || "—") })),
            };
        }

        if (sectionKey === "history") {
            const lines = msg.historyLines || raw.historyLines || [];
            return {
                label: "History",
                columns: [
                    { key: "dateTime", label: "Date Time" },
                    { key: "phase", label: "Phase" },
                    { key: "action", label: "Action" },
                    { key: "reason", label: "Reason" },
                    { key: "entity", label: "Entity" },
                    { key: "channel", label: "Channel" },
                    { key: "user", label: "User" },
                    { key: "comment", label: "Comment" },
                ],
                rows: lines.map((line) => ({
                    dateTime: line.historyDate ? new Date(line.historyDate).toLocaleString("en-US", { year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: true }) : "—",
                    phase: line.phase || "—",
                    action: line.action || "—",
                    reason: line.reason || "—",
                    entity: line.entity || "—",
                    channel: line.channel || "—",
                    user: line.user || "—",
                    comment: line.comment || "—",
                })),
            };
        }

        if (sectionKey === "payload") {
            const lines = msg.block4Fields || raw.mtPayload?.block4Fields || [];
            return {
                label: "Extended text",
                columns: [
                    { key: "tag", label: "Tag" },
                    { key: "label", label: "Field Label" },
                    { key: "rawValue", label: "Raw Value" },
                ],
                rows: lines.map((line) => ({
                    tag: line.tag || "—",
                    label: line.label || (line.tag ? `Tag ${line.tag}` : "—"),
                    rawValue: line.rawValue || "—",
                })),
            };
        }

        if (sectionKey === "rawpayload") {
            return {
                label: "Raw Payload",
                columns: [{ key: "field", label: "Field" }, { key: "value", label: "Value" }],
                rows: [{ field: "Raw Payload", value: stringifyExportValue(getRawPayloadText(msg)) }],
            };
        }

        if (sectionKey === "details") {
            const ordered = getDetailPairs(msg);
            return {
                label: "All Fields",
                columns: [{ key: "field", label: "Field" }, { key: "value", label: "Value" }],
                rows: ordered.map(item => ({ field: item.label, value: stringifyExportValue(item.val) })),
            };
        }

        if (sectionKey === "rawcopies") {
            const rows = (rcDataOverride?.copies || []).map((row) => ({
                messageReference: row.messageReference || "—",
                messageTypeCode: row.messageTypeCode || "—",
                direction: row.direction || "—",
                currentStatus: row.currentStatus || "—",
                senderAddress: row.senderAddress || "—",
                receiverAddress: row.receiverAddress || "—",
                protocol: row.protocol || "—",
                receivedAt: fmtDate(row.receivedAt || row.ampDateReceived),
                inputType: row.inputType || "—",
                source: row.source || "—",
                rawInput: row.rawInput || "—",
            }));
            return {
                label: "Raw Copies",
                columns: [
                    { key: "messageReference", label: "Message Reference" },
                    { key: "messageTypeCode", label: "Type" },
                    { key: "direction", label: "Direction" },
                    { key: "currentStatus", label: "Status" },
                    { key: "senderAddress", label: "Sender" },
                    { key: "receiverAddress", label: "Receiver" },
                    { key: "protocol", label: "Protocol" },
                    { key: "receivedAt", label: "Received At" },
                    { key: "inputType", label: "Input Type" },
                    { key: "source", label: "Source" },
                    { key: "rawInput", label: "Raw Input" },
                ],
                rows,
            };
        }

        return { label: "Section", columns: [{ key: "field", label: "Field" }, { key: "value", label: "Value" }], rows: [] };
    }, [modalRcData, msg, getDisplayType, getDisplayFormat, formatDirection]);

    const exportOrderedModalSectionsPdf = useCallback(async (orderedKeys, rcData) => {
        await loadScriptOnce("https://cdnjs.cloudflare.com/ajax/libs/jspdf/2.5.1/jspdf.umd.min.js", () => !!window.jspdf?.jsPDF);
        await loadScriptOnce("https://cdnjs.cloudflare.com/ajax/libs/jspdf-autotable/3.8.2/jspdf.plugin.autotable.min.js", () => !!window.jspdf?.jsPDF?.API?.autoTable);

        const { jsPDF } = window.jspdf;
        const doc = new jsPDF({ orientation: "portrait", unit: "pt", format: "a4" });
        const margin = { top: 48, right: 34, bottom: 34, left: 34 };
        const pageWidth = doc.internal.pageSize.getWidth();
        const pageHeight = doc.internal.pageSize.getHeight();
        const contentWidth = pageWidth - margin.left - margin.right;
        let cursorY = margin.top;

        const drawPageHeader = () => {
            doc.setFont("helvetica", "normal");
            doc.setFontSize(8);
            doc.text(`Page ${doc.getNumberOfPages()}`, pageWidth - margin.right, 28, { align: "right" });
        };

        const resetPageCursor = () => {
            cursorY = margin.top;
            drawPageHeader();
        };

        const ensureSpace = (neededHeight) => {
            if (cursorY + neededHeight <= pageHeight - margin.bottom) return;
            doc.addPage();
            resetPageCursor();
        };

        const drawSectionHeading = (label) => {
            ensureSpace(20);
            doc.setFont("helvetica", "bold");
            doc.setFontSize(10);
            doc.setTextColor(33, 56, 99);
            doc.text(label, margin.left, cursorY);
            cursorY += 8;
        };

        const drawPairTable = (pairs) => {
            doc.autoTable({
                startY: cursorY,
                margin,
                tableWidth: contentWidth,
                theme: "grid",
                head: [["Field", "Value"]],
                body: pairs.length ? pairs : [["Field", "—"]],
                styles: {
                    fontSize: 8.25,
                    cellPadding: { top: 4, right: 5, bottom: 4, left: 5 },
                    overflow: "linebreak",
                    valign: "top",
                    lineColor: [226, 232, 240],
                    lineWidth: 0.35,
                },
                headStyles: {
                    fillColor: [243, 244, 246],
                    textColor: [55, 65, 81],
                    fontStyle: "bold",
                    fontSize: 8.25,
                },
                columnStyles: {
                    0: { cellWidth: 170, fontStyle: "bold", textColor: [100, 116, 139] },
                    1: { cellWidth: contentWidth - 170, textColor: [17, 24, 39] },
                },
                didDrawPage: () => drawPageHeader(),
            });
            cursorY = (doc.lastAutoTable?.finalY || cursorY) + 12;
        };

        const drawDataTable = (columns, rows) => {
            const safeColumns = columns.filter((column) => {
                if (column.key === "index") return true;
                return rows.some((row) => {
                    const value = stringifyExportValue(row?.[column.key]);
                    return value && value !== "—";
                });
            });
            const widths = estimatePdfColumnWidths(safeColumns, rows);
            const totalWidth = widths.reduce((sum, width) => sum + width, 0) || 1;
            const scale = totalWidth > contentWidth ? contentWidth / totalWidth : 1;
            const scaled = widths.map((width) => Math.max(48, width * scale));
            const fontSize = safeColumns.length > 7 ? 6.5 : safeColumns.length > 4 ? 7.25 : 8;

            doc.autoTable({
                startY: cursorY,
                margin,
                tableWidth: contentWidth,
                theme: "grid",
                head: [safeColumns.map((column) => column.label)],
                body: (rows.length ? rows : [{}]).map((row) => safeColumns.map((column) => stringifyExportValue(row?.[column.key] ?? "—") || "—")),
                styles: {
                    fontSize,
                    cellPadding: { top: 4, right: 4, bottom: 4, left: 4 },
                    overflow: "linebreak",
                    valign: "top",
                    lineColor: [226, 232, 240],
                    lineWidth: 0.35,
                },
                headStyles: {
                    fillColor: [243, 244, 246],
                    textColor: [55, 65, 81],
                    fontStyle: "bold",
                    fontSize,
                },
                columnStyles: Object.fromEntries(scaled.map((width, index) => [index, { cellWidth: width }])),
                didDrawPage: () => drawPageHeader(),
            });
            cursorY = (doc.lastAutoTable?.finalY || cursorY) + 12;
        };

        const drawRawPayloadBlock = (text) => {
            const rawText = stringifyExportValue(text || "—") || "—";
            const lineHeight = 8.5;
            const codePadding = 8;
            const wrappedLines = doc.splitTextToSize(rawText, contentWidth - (codePadding * 2));
            let offset = 0;

            while (offset < wrappedLines.length) {
                ensureSpace(34);
                const availableHeight = pageHeight - margin.bottom - cursorY - (codePadding * 2);
                const linesPerPage = Math.max(1, Math.floor(availableHeight / lineHeight));
                const lineChunk = wrappedLines.slice(offset, offset + linesPerPage);
                const boxHeight = (lineChunk.length * lineHeight) + (codePadding * 2);

                doc.setFillColor(248, 250, 252);
                doc.setDrawColor(226, 232, 240);
                doc.roundedRect(margin.left, cursorY, contentWidth, boxHeight, 6, 6, "FD");
                doc.setFont("courier", "normal");
                doc.setFontSize(7);
                doc.setTextColor(17, 24, 39);
                doc.text(lineChunk, margin.left + codePadding, cursorY + codePadding + 5);

                cursorY += boxHeight + 10;
                offset += linesPerPage;
            }
        };

        resetPageCursor();
        const messageRef = modalRefValue || msg.sequenceNumber || "—";

        ensureSpace(30);
        doc.setFont("helvetica", "bold");
        doc.setFontSize(11);
        doc.setTextColor(17, 24, 39);
        doc.text(`Message Ref: ${messageRef}`, margin.left, cursorY);
        cursorY += 14;

        const summaryLine = [getDisplayType(msg), getDisplayFormat(msg), formatDirection(msg.io || msg.direction)].filter(Boolean).join("   ");
        if (summaryLine) {
            doc.setFont("helvetica", "normal");
            doc.setFontSize(8);
            doc.setTextColor(100, 116, 139);
            doc.text(summaryLine, margin.left, cursorY);
            cursorY += 10;
        }

        doc.setDrawColor(203, 213, 225);
        doc.line(margin.left, cursorY, pageWidth - margin.right, cursorY);
        cursorY += 14;

        orderedKeys.forEach((targetKey) => {
            const block = getSectionData(targetKey, rcData);
            drawSectionHeading(block.label);

            if (targetKey === "rawpayload") {
                drawRawPayloadBlock(getRawPayloadText(msg));
                return;
            }

            const blockColumns = targetKey === "rawcopies"
                ? block.columns.filter((column) => !["senderAddress", "receiverAddress", "protocol", "receivedAt", "inputType", "source"].includes(column.key))
                : block.columns;
            const isPairBlock = blockColumns.length === 2 && blockColumns.some(col => col.key === "field") && blockColumns.some(col => col.key === "value");

            if (isPairBlock) {
                drawPairTable(block.rows.map((row) => [stringifyExportValue(row.field || "—"), stringifyExportValue(row.value || "—")]));
                return;
            }

            if (block.rows.length === 1 && blockColumns.length > 4) {
                const single = block.rows[0] || {};
                drawPairTable(blockColumns.map((column) => [column.label, stringifyExportValue(single[column.key] ?? "—") || "—"]));
                return;
            }

            drawDataTable(blockColumns, block.rows);
        });

        doc.save(`${modalBaseName}_${safeFileNamePart(orderedKeys.map(key => getSectionData(key, rcData).label).join("_").toLowerCase().replace(/\s+/g, "_"))}.pdf`);
    }, [getSectionData, getDisplayType, getDisplayFormat, msg, modalBaseName, modalRefValue, safeFileNamePart]);

    const exportOrderedModalSectionsWord = useCallback((orderedKeys, rcData) => {
        const summaryLine = [getDisplayType(msg), getDisplayFormat(msg), formatDirection(msg.io || msg.direction)].filter(Boolean).join("   ");
        const sections = orderedKeys.map((targetKey) => {
            const block = getSectionData(targetKey, rcData);
            const blockColumns = targetKey === "rawcopies"
                ? block.columns.filter((column) => !["senderAddress", "receiverAddress", "protocol", "receivedAt", "inputType", "source"].includes(column.key))
                : block.columns;
            const isPairBlock = blockColumns.length === 2 && blockColumns.some(col => col.key === "field") && blockColumns.some(col => col.key === "value");

            if (targetKey === "rawpayload") {
                return { label: block.label, type: "raw", rawText: stringifyExportValue(getRawPayloadText(msg) || "—") || "—" };
            }

            if (isPairBlock) {
                return {
                    label: block.label,
                    type: "pair",
                    columns: blockColumns,
                    rows: block.rows.map((row) => ({
                        field: stringifyExportValue(row.field || "—") || "—",
                        value: stringifyExportValue(row.value || "—") || "—",
                    })),
                };
            }

            if (block.rows.length === 1 && blockColumns.length > 4) {
                const single = block.rows[0] || {};
                return {
                    label: block.label,
                    type: "pair",
                    columns: [{ key: "field", label: "Field" }, { key: "value", label: "Value" }],
                    rows: blockColumns.map((column) => ({
                        field: column.label,
                        value: stringifyExportValue(single[column.key] ?? "—") || "—",
                    })),
                };
            }

            const safeColumns = getWordRenderableColumns(blockColumns, block.rows);
            return {
                label: block.label,
                type: "table",
                columns: safeColumns,
                rows: (block.rows.length ? block.rows : [{}]).map((row) => Object.fromEntries(
                    safeColumns.map((column) => [column.key, stringifyExportValue(row?.[column.key] ?? "—") || "—"])
                )),
            };
        });

        const html = buildWordComponentExportHtml({
            title: `${orderedKeys.map(key => getSectionData(key, rcData).label).join(" + ")} - ${getDisplayType(msg) || "Message"}`,
            documents: [{
                pageLabel: "Page 1",
                messageRef: modalRefValue || msg.sequenceNumber || "—",
                summaryLine,
                sections,
            }],
        });

        triggerWordDownload(
            html,
            `${modalBaseName}_${safeFileNamePart(orderedKeys.map(key => getSectionData(key, rcData).label).join("_").toLowerCase().replace(/\s+/g, "_"))}`
        );
    }, [formatDirection, getDisplayFormat, getDisplayType, getSectionData, modalBaseName, modalRefValue, msg, safeFileNamePart]);

    const buildOrderedModalSectionJson = useCallback((orderedKeys, rcData) => {
        const meta = {
            messageReference: modalRefValue || msg.reference || msg.messageReference || msg.sequenceNumber || "â€”",
            messageType: getDisplayType(msg) || msg.messageCode || "â€”",
            messageFormat: getDisplayFormat(msg) || "â€”",
        };

        return {
            messageReference: meta.messageReference,
            sections: orderedKeys.map((targetKey) => {
                const block = getSectionData(targetKey, rcData);
                return {
                    section: block.label,
                    data: buildSectionJsonData({ targetKey, block, meta }),
                };
            }),
        };
    }, [getSectionData, getDisplayFormat, getDisplayType, modalRefValue, msg]);

    const exportModalData = async (targetKeys, format) => {
        setModalExportOpen(false);
        setModalExporting(true);
        try {
            const orderedKeys = Array.isArray(targetKeys) && targetKeys.length ? targetKeys : [tab];
            const resolvedKey = orderedKeys.length === 1 ? orderedKeys[0] : "";
            if (format === "word" && isWordExportDisabledForTargets(orderedKeys)) {
                throw new Error("Word export is unavailable when Raw Copies is selected.");
            }
            if (MT_RAW_ONLY_EXPORT_FORMATS.has(format)) {
                if (orderedKeys.length !== 1 || resolvedKey !== "rawpayload") throw new Error("RJE / DOSPCC export is available only when Raw Payload is the only selected section.");
                exportMtRawPayloads({
                    format,
                    messages: [msg],
                    fileBaseName: `${modalBaseName}_raw_payload`,
                });
                notify(`Exported Raw Payload as ${format.toUpperCase()}`);
                return;
            }

            let rcData = modalRcData;
            if (orderedKeys.includes("rawcopies") && !rcData && modalRefValue) {
                rcData = await fetchModalRawCopies();
                setModalRcData(rcData);
            }

            if (format === "json") {
                const mergedLabel = orderedKeys.map(key => getSectionData(key, rcData).label).join(" + ");
                const payload = buildOrderedModalSectionJson(orderedKeys, rcData);
                triggerDownload(
                    new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" }),
                    `${modalBaseName}_${safeFileNamePart(mergedLabel.toLowerCase().replace(/\s+/g, "_"))}.json`
                );
                notify(`Exported ${mergedLabel} as JSON`);
                return;
            }

            if (format === "pdf") {
                await exportOrderedModalSectionsPdf(orderedKeys, rcData);
                notify(`Exported ${orderedKeys.map(key => getSectionData(key, rcData).label).join(" + ")} as PDF`);
            } else if (format === "word") {
                exportOrderedModalSectionsWord(orderedKeys, rcData);
                notify(`Exported ${orderedKeys.map(key => getSectionData(key, rcData).label).join(" + ")} as Word`);
            } else if (orderedKeys.length > 1) {
                const blocks = orderedKeys.map(k => ({ ...getSectionData(k, rcData) }));
                const mergedRows = [];
                const keySet = new Set(["section"]);
                blocks.forEach(block => {
                    block.rows.forEach((row) => {
                        const merged = { section: block.label };
                        Object.entries(row).forEach(([k, v]) => { merged[k] = v; keySet.add(k); });
                        mergedRows.push(merged);
                    });
                });
                const columns = [...keySet].map(k => ({ key: k, label: toPrettyLabel(k) }));
                const mergedLabel = orderedKeys.map(key => getSectionData(key, rcData).label).join(" + ");
                await exportRowsAsFile({
                    format,
                    rows: mergedRows,
                    columns,
                    fileBaseName: `${modalBaseName}_${safeFileNamePart(mergedLabel.toLowerCase().replace(/\s+/g, "_"))}`,
                    title: `${mergedLabel} - ${getDisplayType(msg) || "Message"}`,
                    sheetName: mergedLabel,
                    showPdfTitle: false,
                });
                notify(`Exported ${mergedLabel} as ${format.toUpperCase()}`);
            } else {
                const section = getSectionData(resolvedKey, rcData);
                await exportRowsAsFile({
                    format,
                    rows: section.rows,
                    columns: section.columns,
                    fileBaseName: `${modalBaseName}_${safeFileNamePart(section.label.toLowerCase().replace(/\s+/g, "_"))}`,
                    title: `${section.label} - ${getDisplayType(msg) || "Message"}`,
                    sheetName: section.label,
                    showPdfTitle: false,
                });
                notify(`Exported ${section.label} as ${format.toUpperCase()}`);
            }
        } catch (e) {
            notify(e.message || "Popup export failed", "error");
        } finally {
            setModalExporting(false);
        }
    };

    // Sync live refs only when React state changes (not during drag)
    useEffect(() => {
        livePos.current  = { x: pos.x,  y: pos.y  };
        liveSize.current = { w: size.w, h: size.h };
    }, [pos.x, pos.y, size.w, size.h]);

    // Direct DOM update — zero React involvement, zero re-renders, zero blink
    const applyDOM = (x, y, w, h) => {
        const el = boxRef.current;
        if (!el) return;
        el.style.left   = x + "px";
        el.style.top    = y + "px";
        el.style.width  = w + "px";
        el.style.height = h + "px";
        const body = el.querySelector(".fm-body");
        if (body) body.style.height = Math.max(80, h - 170) + "px";
    };

    // ── Drag ─────────────────────────────────────────────────────────────
    const onDragStart = (e) => {
        if (e.button !== 0) return;
        e.preventDefault();
        // z-index update via direct DOM — no React state
        const el = boxRef.current;
        if (el) el.style.zIndex = (parseInt(el.style.zIndex || 1000) + 1);
        dragRef.current = {
            ox: e.clientX - livePos.current.x,
            oy: e.clientY - livePos.current.y,
        };
        document.body.style.userSelect = "none";
        document.body.style.cursor     = "grabbing";

        const onMove = (ev) => {
            if (!dragRef.current) return;
            const nx = ev.clientX - dragRef.current.ox;
            const ny = ev.clientY - dragRef.current.oy;
            const cx = Math.max(0, Math.min(window.innerWidth  - liveSize.current.w, nx));
            const cy = Math.max(0, Math.min(window.innerHeight - 60, ny));
            livePos.current = { x: cx, y: cy };
            applyDOM(cx, cy, liveSize.current.w, liveSize.current.h);
        };
        const onUp = () => {
            if (!dragRef.current) return;
            dragRef.current = null;
            document.body.style.userSelect = "";
            document.body.style.cursor     = "";
            // Commit final position to React state ONCE on mouseup
            onPatch(id, { pos: { ...livePos.current } });
            // Sync z-index to React state once too
            const z = el ? parseInt(el.style.zIndex) : 1001;
            onBringToFront(id, z);
            window.removeEventListener("mousemove", onMove);
            window.removeEventListener("mouseup",   onUp);
        };
        window.addEventListener("mousemove", onMove);
        window.addEventListener("mouseup",   onUp);
    };

    // ── Resize ────────────────────────────────────────────────────────────
    const MIN_W = 500, MIN_H = 360;

    const onResizeStart = (e, dir) => {
        if (e.button !== 0) return;
        e.preventDefault();
        e.stopPropagation();
        resRef.current = {
            sx: e.clientX, sy: e.clientY,
            sw: liveSize.current.w, sh: liveSize.current.h,
            spx: livePos.current.x, spy: livePos.current.y,
            dir,
        };
        document.body.style.userSelect = "none";

        const onMove = (ev) => {
            if (!resRef.current) return;
            const { sx, sy, sw, sh, spx, spy, dir: d } = resRef.current;
            const dx = ev.clientX - sx, dy = ev.clientY - sy;
            let nw = sw, nh = sh, nx = spx, ny = spy;
            if (d.includes("e")) nw = Math.max(MIN_W, sw + dx);
            if (d.includes("s")) nh = Math.max(MIN_H, sh + dy);
            if (d.includes("w")) { nw = Math.max(MIN_W, sw - dx); nx = spx + (sw - nw); }
            if (d.includes("n")) { nh = Math.max(MIN_H, sh - dy); ny = spy + (sh - nh); }
            liveSize.current = { w: nw, h: nh };
            livePos.current  = { x: nx, y: ny };
            applyDOM(nx, ny, nw, nh);
        };
        const onUp = () => {
            if (!resRef.current) return;
            resRef.current = null;
            document.body.style.userSelect = "";
            // Commit ONCE on mouseup
            onPatch(id, {
                pos:  { ...livePos.current  },
                size: { ...liveSize.current },
            });
            window.removeEventListener("mousemove", onMove);
            window.removeEventListener("mouseup",   onUp);
        };
        window.addEventListener("mousemove", onMove);
        window.addEventListener("mouseup",   onUp);
    };

    const handles = [
        { d:"n",  s:{top:0,left:8,right:8,height:6,cursor:"n-resize"} },
        { d:"s",  s:{bottom:0,left:8,right:8,height:6,cursor:"s-resize"} },
        { d:"e",  s:{right:0,top:8,bottom:8,width:6,cursor:"e-resize"} },
        { d:"w",  s:{left:0,top:8,bottom:8,width:6,cursor:"w-resize"} },
        { d:"ne", s:{top:0,right:0,width:14,height:14,cursor:"ne-resize"} },
        { d:"nw", s:{top:0,left:0,width:14,height:14,cursor:"nw-resize"} },
        { d:"se", s:{bottom:0,right:0,width:14,height:14,cursor:"se-resize"} },
        { d:"sw", s:{bottom:0,left:0,width:14,height:14,cursor:"sw-resize"} },
    ];

    const bodyH = Math.max(80, size.h - 170);

    // Bring to front on window click — direct DOM only, no state
    const handleWindowMouseDown = () => {
        const el = boxRef.current;
        if (el) el.style.zIndex = (parseInt(el.style.zIndex || 1000) + 1);
    };

    return (
        <div
            ref={boxRef}
            className="fm-window"
            style={{ left: pos.x, top: pos.y, width: size.w, height: size.h, zIndex }}
            onMouseDown={handleWindowMouseDown}
        >
            {handles.map(h => (
                <div key={h.d} className="fm-resize-handle"
                    style={{ position:"absolute", zIndex:5, ...h.s }}
                    onMouseDown={e => onResizeStart(e, h.d)}
                />
            ))}

            <div className="txn-header fm-drag-header" onMouseDown={onDragStart}>
                <div className="txn-header-left">
                    <div className="txn-type-pill">{getDisplayFormat(msg)}</div>
                    <div className="txn-meta-wrap">
                        <div className="txn-meta-line">
                            {[
                                (msg.statusDate || msg.creationDate)
                                    ? new Date(msg.statusDate || msg.creationDate).toLocaleString("en-GB", {
                                        year: "numeric",
                                        month: "2-digit",
                                        day: "2-digit",
                                        hour: "2-digit",
                                        minute: "2-digit",
                                        second: "2-digit",
                                        hour12: false,
                                    })
                                    : "—",
                                formatDirection(msg.io || msg.direction),
                                msg.phase || "—",
                                msg.action || "—",
                                msg.status || "—",
                            ].join(" / ")}
                        </div>
                    </div>
                </div>
                <div className="txn-header-right" onMouseDown={e => e.stopPropagation()}>
                    <div className="txn-nav">
                        <button className="txn-nav-btn" onClick={()=>onPrev(id)} disabled={isFirst}>
                            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round"><polyline points="15 18 9 12 15 6"/></svg>
                        </button>
                        <span className="txn-nav-count">{index+1}/{processed.length}</span>
                        <button className="txn-nav-btn" onClick={()=>onNext(id)} disabled={isLast}>
                            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round"><polyline points="9 18 15 12 9 6"/></svg>
                        </button>
                    </div>
                    <div className="txn-export-wrap" ref={modalExportRef}>
                        <button className="txn-export-btn" onClick={()=>{ if (!modalExporting) { setModalExportTargets([tab]); setModalExportOpen(p=>!p); } }} disabled={modalExporting}>
                            {modalExporting ? "Exporting…" : "Export"}
                            <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><polyline points="6 9 12 15 18 9"/></svg>
                        </button>
                        {modalExportOpen && (
                            <div className="txn-export-menu" onMouseDown={e=>e.stopPropagation()}>
                                <div className="txn-export-scope-label">Export Section</div>
                                <div className="txn-export-scope-grid">
                                    {[
                                    { key: "header", label: "Header" },
                                    { key: "rawpayload", label: "Raw Payload" },
                                    { key: "payload", label: "Extended text" },
                                        { key: "history", label: "History" },
                                    { key: "details", label: "All Fields" },
                                    ].map(target => {
                                        const order = orderedModalExportTargets.indexOf(target.key) + 1;
                                        return (
                                        <button key={target.key} className={`txn-export-scope-btn${order?" active":""}`} onClick={()=>toggleModalExportTarget(target.key)}>
                                            <span>{target.label}</span>
                                            {order ? <span className="txn-export-order-badge">{order}</span> : null}
                                        </button>
                                    );})}
                                </div>
                                <div className="txn-export-format-label">Format</div>
                                <div className="txn-export-format-grid">
                                    {modalExportFormats.map((option) => (
                                        <button
                                            key={option.key}
                                            className="txn-export-opt"
                                            onClick={()=>exportModalData(orderedModalExportTargets, option.key)}
                                            disabled={option.disabled}
                                            title={option.disabledReason || undefined}
                                        >
                                            {option.iconText}
                                        </button>
                                    ))}
                                </div>
                            </div>
                        )}
                    </div>
                    <button className="txn-close" onClick={()=>onClose(id)}>
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
                    </button>
                </div>
            </div>

            {false && <div className="txn-summary-strip">
                <div className="txn-summary-item"><span className="txn-sum-label">Sender</span><span className="txn-sum-value mono">{msg.sender||"—"}</span></div>
                <div className="txn-summary-arrow"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round"><line x1="5" y1="12" x2="19" y2="12"/><polyline points="12 5 19 12 12 19"/></svg></div>
                <div className="txn-summary-item"><span className="txn-sum-label">Receiver</span><span className="txn-sum-value mono">{msg.receiver||"—"}</span></div>
                <div className="txn-summary-divider"/>
                <div className="txn-summary-item"><span className="txn-sum-label">Country</span><span className="txn-sum-value">{msg.country||"—"}</span></div>
                <div className="txn-summary-divider"/>
                <div className="txn-summary-item"><span className="txn-sum-label">Owner</span><span className="txn-sum-value">{msg.owner||msg.ownerUnit||"—"}</span></div>
                <div className="txn-summary-divider"/>
                <div className="txn-summary-item"><span className="txn-sum-label">Network</span><span className="txn-sum-value">{msg.networkProtocol||msg.network||"—"}</span></div>
                <div className="txn-summary-item"><span className="txn-sum-label">Direction</span><span className={"dir-badge "+dirClass(msg.io||msg.direction)}>{formatDirection(msg.io||msg.direction)}</span></div>
            </div>}

            {(()=>{
                const headerPairs = getHeaderPairs(msg);
                const headerRows = [];
                for (let i = 0; i < headerPairs.length; i += 2) {
                    headerRows.push(headerPairs.slice(i, i + 2));
                }
                return (
                    <div className="txn-fixed-header txn-section-wrap">
                        <div className="txn-fixed-header-title">Header</div>
                        <div className="txn-fixed-header-card">
                            {headerRows.map((row,rowIdx)=>(
                                <div key={`hdr-fixed-row-${rowIdx}`} className="txn-fixed-header-row" style={{borderBottom:rowIdx<headerRows.length-1?"1px solid var(--gray-6)":"none"}}>
                                    {row.map((item,colIdx)=>(
                                        <div key={item.key || item.label} className="txn-fixed-header-cell" style={{borderRight:colIdx===0&&row.length>1?"1px solid var(--gray-6)":"none"}}>
                                            <div className="txn-fixed-header-label">{item.label}:</div>
                                            <div style={{padding:"11px 16px",fontSize:14,fontWeight:600,color:"var(--black)",fontFamily:/reference|type/i.test(item.key||"")?"var(--font-mono)":"inherit",wordBreak:"break-all",display:"flex",alignItems:"center"}}>{stringifyExportValue(item.val)||"â€”"}</div>
                                        </div>
                                    ))}
                                    {row.length===1&&<div />}
                                </div>
                            ))}
                        </div>
                    </div>
                );
            })()}

            <div className="txn-tabs">
                {modalTabs.map(t=>(
                    <button key={t.key}
                        className={"txn-tab"+(tab===t.key?" txn-tab-active":"")}
                        onClick={()=>onPatch(id,{tab:t.key})}
                    >
                        {t.label}
                        {t.count ? <span className="txn-tab-count">{t.count}</span> : null}
                    </button>
                ))}
            </div>

            <div className="txn-body fm-body" style={{height:bodyH,overflowY:"auto",overflowX:"hidden"}}>
                {false&&tab==="header"&&(()=>{
                    const headerPairs = getHeaderPairs(msg);
                    const headerRows = [];
                    for (let i = 0; i < headerPairs.length; i += 2) {
                        headerRows.push(headerPairs.slice(i, i + 2));
                    }
                    return <div className="txn-section-wrap">
                        <div style={{border:"1px solid var(--gray-6)",borderRadius:8,overflow:"hidden",background:"var(--white)"}}>
                            {headerRows.map((row,rowIdx)=>(
                                <div key={`hdr-row-${rowIdx}`} style={{display:"grid",gridTemplateColumns:"1fr 1fr",borderBottom:rowIdx<headerRows.length-1?"1px solid var(--gray-6)":"none",background:rowIdx%2===0?"var(--white)":"var(--gray-7)"}}>
                                    {row.map((item,colIdx)=>(
                                        <div key={item.key || item.label} style={{display:"grid",gridTemplateColumns:"160px 1fr",borderRight:colIdx===0&&row.length>1?"1px solid var(--gray-6)":"none"}}>
                                            <div style={{padding:"11px 16px",fontSize:13,color:"var(--gray-2)",display:"flex",alignItems:"center"}}>{item.label}:</div>
                                            <div style={{padding:"11px 16px",fontSize:14,fontWeight:600,color:"var(--black)",fontFamily:/reference|type/i.test(item.key||"")?"var(--font-mono)":"inherit",wordBreak:"break-all",display:"flex",alignItems:"center"}}>{stringifyExportValue(item.val)||"—"}</div>
                                        </div>
                                    ))}
                                    {row.length===1&&<div />}
                                </div>
                            ))}
                        </div>
                    </div>;
                })()}

                {tab==="body"&&<div className="txn-section-wrap"><div className="txn-fields-grid">
                    <div className="txn-field"><span className="txn-field-label">Message Code</span><span className="txn-field-value">{msg.messageCode||getDisplayType(msg)||"—"}</span></div>
                    <div className="txn-field"><span className="txn-field-label">Message Type</span><span className="txn-field-value">{getDisplayFormat(msg)||"—"}</span></div>
                    <div className="txn-field"><span className="txn-field-label">Network Protocol</span><span className="txn-field-value">{msg.networkProtocol||msg.network||"—"}</span></div>
                    <div className="txn-field"><span className="txn-field-label">Network Channel</span><span className="txn-field-value">{msg.networkChannel||msg.backendChannel||"—"}</span></div>
                    <div className="txn-field"><span className="txn-field-label">Network Priority</span><span className="txn-field-value">{msg.networkPriority||"—"}</span></div>
                    <div className="txn-field"><span className="txn-field-label">Country</span><span className="txn-field-value">{msg.country||"—"}</span></div>
                    <div className="txn-field"><span className="txn-field-label">Owner</span><span className="txn-field-value">{msg.owner||msg.ownerUnit||"—"}</span></div>
                    <div className="txn-field"><span className="txn-field-label">Workflow</span><span className="txn-field-value">{msg.workflow||"—"}</span></div>
                    <div className="txn-field"><span className="txn-field-label">Direction</span><span className={"txn-field-value dir-badge "+dirClass(msg.io||msg.direction)}>{formatDirection(msg.io||msg.direction)}</span></div>
                    <div className="txn-field"><span className="txn-field-label">Status</span><span className={"txn-field-value "+statusCls(msg.status)}>{msg.status||"—"}</span></div>
                    <div className="txn-field"><span className="txn-field-label">Phase</span><span className="txn-field-value">{msg.phase||"—"}</span></div>
                    <div className="txn-field"><span className="txn-field-label">Action</span><span className="txn-field-value">{msg.action||"—"}</span></div>
                    <div className="txn-field"><span className="txn-field-label">Reason</span><span className="txn-field-value">{msg.reason||"—"}</span></div>
                    <div className="txn-field"><span className="txn-field-label">Environment</span><span className="txn-field-value">{msg.environment||"—"}</span></div>
                    <div className="txn-field"><span className="txn-field-label">Session No.</span><span className="txn-field-value mono">{msg.sessionNumber||"—"}</span></div>
                    <div className="txn-field"><span className="txn-field-label">Sequence No.</span><span className="txn-field-value mono">{msg.sequenceNumber||"—"}</span></div>
                </div></div>}

                {tab==="history"&&<div className="txn-section-wrap">
                    {(()=>{
                        if(showDetailLoading) return <div className="adv-empty-state"><span className="spinner" style={{width:24,height:24,borderWidth:3,borderTopColor:"var(--accent)"}}/><p>Loading history...</p></div>;
                        const lines = msg.historyLines || msg.rawMessage?.historyLines || [];
                        if(lines.length===0) return <div className="adv-empty-state"><svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="var(--gray-4)" strokeWidth="1.5"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg><p>No history lines available</p></div>;
                        return (
                            <div style={{overflowX:"auto"}}>
                                <table className="history-table" style={{width:"max-content",minWidth:"100%",borderCollapse:"collapse",fontSize:13,tableLayout:"auto"}}>
                                    <thead style={{position:"sticky",top:0,zIndex:10}}>
                                        <tr style={{background:"var(--gray-7)",borderBottom:"2px solid var(--gray-5)"}}>
                                            <th style={{padding:"12px 16px",textAlign:"left",fontWeight:600,color:"var(--gray-2)",whiteSpace:"nowrap",minWidth:180}}>Date & Time</th>
                                            <th style={{padding:"12px 16px",textAlign:"left",fontWeight:600,color:"var(--gray-2)",whiteSpace:"nowrap",minWidth:110}}>Phase</th>
                                            <th style={{padding:"12px 16px",textAlign:"left",fontWeight:600,color:"var(--gray-2)",whiteSpace:"nowrap",minWidth:110}}>Action</th>
                                            <th style={{padding:"12px 16px",textAlign:"left",fontWeight:600,color:"var(--gray-2)",whiteSpace:"nowrap",minWidth:100}}>Reason</th>
                                            <th style={{padding:"12px 16px",textAlign:"left",fontWeight:600,color:"var(--gray-2)",whiteSpace:"nowrap",minWidth:130}}>Entity</th>
                                            <th style={{padding:"12px 16px",textAlign:"left",fontWeight:600,color:"var(--gray-2)",whiteSpace:"nowrap",minWidth:120}}>Channel</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {lines.map((line,idx)=>(
                                            <tr key={idx} style={{borderBottom:"1px solid var(--gray-6)",background:idx%2===0?"var(--white)":"var(--gray-7)"}}>
                                                <td style={{padding:"10px 16px",fontFamily:"monospace",fontSize:12,color:"var(--black-3)",whiteSpace:"nowrap"}}>{line.historyDate?new Date(line.historyDate).toLocaleString("en-US",{year:"numeric",month:"2-digit",day:"2-digit",hour:"2-digit",minute:"2-digit",second:"2-digit",hour12:true}):"—"}</td>
                                                <td style={{padding:"10px 16px",whiteSpace:"nowrap"}}>{line.phase?<span className="txn-status-badge" style={{fontSize:11,padding:"3px 9px"}}>{line.phase}</span>:"—"}</td>
                                                <td style={{padding:"10px 16px",whiteSpace:"nowrap"}}>{line.action?<span className={"txn-status-badge "+(line.action==="Delivered"?"badge-ok":line.action==="Rejected"?"badge-bypass":"badge-pending")} style={{fontSize:11,padding:"3px 9px"}}>{line.action}</span>:"—"}</td>
                                                <td style={{padding:"10px 16px",color:"var(--black-3)",whiteSpace:"nowrap"}}>{line.reason||"—"}</td>
                                                <td style={{padding:"10px 16px",whiteSpace:"nowrap"}}>{line.entity?<span className="txn-status-badge" style={{fontSize:11,padding:"3px 9px",background:"var(--accent-light)",color:"var(--accent)"}}>{line.entity}</span>:"—"}</td>
                                                <td style={{padding:"10px 16px",fontFamily:"monospace",fontSize:12,color:"var(--black-3)",whiteSpace:"nowrap"}}>{line.channel||"—"}</td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        );
                    })()}
                </div>}

                {tab==="payload"&&<div className="txn-section-wrap txn-payload-wrap">
                    {(()=>{
                        if(showDetailLoading) return <div className="adv-empty-state"><span className="spinner" style={{width:24,height:24,borderWidth:3,borderTopColor:"var(--accent)"}}/><p>Loading Extended text...</p></div>;
                        const lines = msg.block4Fields || msg.rawMessage?.mtPayload?.block4Fields || [];
                        if(lines.length===0) return <div className="adv-empty-state"><svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="var(--gray-4)" strokeWidth="1.5"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg><p>No Extended text fields available</p></div>;
                        return (
                            <div style={{overflowX:"auto"}}>
                                <table className="history-table" style={{width:"max-content",minWidth:"100%",borderCollapse:"collapse",fontSize:13,tableLayout:"auto"}}>
                                    <thead style={{position:"sticky",top:0,zIndex:10}}>
                                        <tr style={{background:"var(--gray-7)",borderBottom:"2px solid var(--gray-5)"}}>
                                            <th style={{padding:"12px 16px",textAlign:"left",fontWeight:600,color:"var(--gray-2)",whiteSpace:"nowrap",minWidth:60}}>Tag</th>
                                            <th style={{padding:"12px 16px",textAlign:"left",fontWeight:600,color:"var(--gray-2)",whiteSpace:"nowrap",minWidth:220}}>Field Label</th>
                                            <th style={{padding:"12px 16px",textAlign:"left",fontWeight:600,color:"var(--gray-2)",whiteSpace:"nowrap",minWidth:300}}>Raw Value</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {lines.map((line,idx)=>(
                                            <tr key={idx} style={{borderBottom:"1px solid var(--gray-6)",background:idx%2===0?"var(--white)":"var(--gray-7)"}}>
                                                <td style={{padding:"10px 16px",whiteSpace:"nowrap"}}>
                                                    <span className="txn-status-badge" style={{fontSize:11,padding:"3px 8px",fontFamily:"monospace",letterSpacing:"0.04em"}}>{line.tag||"—"}</span>
                                                </td>
                                                <td style={{padding:"10px 16px",color:"var(--black-3)",whiteSpace:"nowrap"}}>{line.label||(line.tag ? `Tag ${line.tag}` : "—")}</td>
                                                <td style={{padding:"10px 16px",fontFamily:"monospace",fontSize:11,color:"var(--black-3)",whiteSpace:"pre-wrap",wordBreak:"break-all",maxWidth:320}}>{line.rawValue||"—"}</td>
                                                {false && <td style={{padding:"10px 16px",fontSize:11,color:"var(--gray-2)"}}>
                                                    {line.components&&Object.keys(line.components).length>0
                                                        ?<div style={{display:"flex",flexDirection:"column",gap:2}}>
                                                            {Object.entries(line.components).map(([ck,cv])=>(
                                                                <div key={ck} style={{display:"flex",gap:6}}>
                                                                    <span style={{color:"var(--gray-4)",minWidth:80,fontWeight:500}}>{ck}:</span>
                                                                    <span style={{fontFamily:"monospace",wordBreak:"break-all"}}>{String(cv||"")}</span>
                                                                </div>
                                                            ))}
                                                         </div>
                                                        :"—"
                                                    }
                                                </td>}
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        );
                    })()}
                </div>}

                {tab==="rawpayload"&&<div className="txn-section-wrap">
                    {(()=>{
                        if(showDetailLoading) return <div className="adv-empty-state"><span className="spinner" style={{width:24,height:24,borderWidth:3,borderTopColor:"var(--accent)"}}/><p>Loading raw payload...</p></div>;
                        const rawPayloadText = getRawPayloadText(msg);
                        if(!rawPayloadText || rawPayloadText === "—") return <div className="adv-empty-state"><svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="var(--gray-4)" strokeWidth="1.5"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg><p>No raw payload available</p></div>;
                        return (
                            <div className="txn-raw-payload-card">
                                <div className="txn-raw-payload-head">Raw Payload</div>
                                <pre className="txn-raw-payload-text">{rawPayloadText}</pre>
                            </div>
                        );
                    })()}
                </div>}

                {tab==="details"&&<div className="txn-section-wrap">
                    {(()=>{
                        if(showDetailLoading) return <div className="adv-empty-state"><span className="spinner" style={{width:24,height:24,borderWidth:3,borderTopColor:"var(--accent)"}}/><p>Loading fields...</p></div>;
                        const ordered = getDetailPairs(msg);
                        if(!ordered.length) return <div style={{padding:40,textAlign:"center",color:"var(--gray-3)"}}><p>No fields available</p></div>;
                        const monoKeys=new Set(["id","mur","uetr","reference","transactionReference","creationDate","receivedDT","statusDate","sessionNumber","sequenceNumber","logicalTerminalAddress","applicationId","serviceId","bankOperationCode"]);
                        const renderVal=(key,val)=>{
                            if(key==="status") return <span className={statusCls(String(val))}>{String(val)}</span>;
                            if(key==="io") return <span className={"dir-badge "+dirClass(String(val))}>{formatDirection(String(val))}</span>;
                            if(typeof val==="object") return <span style={{fontFamily:"monospace",fontSize:12,wordBreak:"break-all"}}>{JSON.stringify(val)}</span>;
                            return <span style={monoKeys.has(key)?{fontFamily:"monospace",fontSize:13,wordBreak:"break-all"}:{wordBreak:"break-word"}}>{String(val)}</span>;
                        };
                        const pairs=[];for(let i=0;i<ordered.length;i+=2)pairs.push([ordered[i],ordered[i+1]||null]);
                        return (
                            <div style={{display:"flex",flexDirection:"column",gap:0}}>
                                {pairs.map((pair,pi)=>(
                                    <div key={pi} style={{display:"grid",gridTemplateColumns:"1fr 1fr",borderBottom:"1px solid var(--gray-6)"}}>
                                        {pair.map((item,ci)=>item?(
                                            <div key={item.key} style={{padding:"14px 20px",borderRight:ci===0?"1px solid var(--gray-6)":"none",background:"var(--white)"}}>
                                                <div style={{fontSize:10,fontWeight:700,letterSpacing:"0.07em",color:"var(--gray-3)",textTransform:"uppercase",marginBottom:5}}>{item.label}</div>
                                                <div style={{fontSize:13,color:"var(--black)",lineHeight:1.5}}>{renderVal(item.key,item.val)}</div>
                                            </div>
                                        ):(
                                            <div key={"e"+pi+ci} style={{padding:"14px 20px",background:"var(--white)"}}/>
                                        ))}
                                    </div>
                                ))}
                            </div>
                        );
                    })()}
                </div>}

                {tab==="rawcopies"&&<div className="txn-section-wrap">
                    {modalRcLoading && (
                        <div style={{padding:40,textAlign:"center",color:"var(--gray-3)",display:"flex",flexDirection:"column",alignItems:"center",gap:12}}>
                            <span className="spinner" style={{width:24,height:24,borderWidth:3,borderTopColor:"var(--accent)"}}/>
                            <p style={{margin:0,fontSize:13}}>Loading raw copies…</p>
                        </div>
                    )}
                    {modalRcError && (
                        <div style={{padding:"12px 16px",background:"var(--danger-light)",borderRadius:6,margin:16,fontSize:13,color:"var(--danger)",border:"1px solid var(--danger-border)"}}>
                            ⚠ {modalRcError}
                        </div>
                    )}
                    {!modalRcLoading && !modalRcError && modalRcData && (
                        <>
                            <div style={{display:"flex",alignItems:"center",gap:16,padding:"12px 16px 0",flexWrap:"wrap"}}>
                                <span style={{fontSize:12,fontWeight:700,color:"var(--gray-2)"}}>{modalRcData.count || modalRcData.copies?.length || 0} Raw {(modalRcData.count||modalRcData.copies?.length||0)===1?"Copy":"Copies"}</span>
                            </div>
                            <div style={{overflowX:"auto",padding:"12px 0 0"}}>
                                <table style={{width:"100%",borderCollapse:"collapse",fontSize:13}}>
                                    <thead>
                                        <tr style={{background:"var(--gray-7)",borderBottom:"2px solid var(--gray-5)"}}>
                                            <th style={{padding:"10px 14px",textAlign:"center",fontWeight:600,color:"var(--gray-2)",width:36}}/>
                                            <th style={{padding:"10px 14px",textAlign:"left",fontWeight:600,color:"var(--gray-2)",whiteSpace:"nowrap"}}>Message Reference</th>
                                            <th style={{padding:"10px 14px",textAlign:"left",fontWeight:600,color:"var(--gray-2)",whiteSpace:"nowrap"}}>Type</th>
                                            <th style={{padding:"10px 14px",textAlign:"left",fontWeight:600,color:"var(--gray-2)",whiteSpace:"nowrap"}}>Direction</th>
                                            <th style={{padding:"10px 14px",textAlign:"left",fontWeight:600,color:"var(--gray-2)",whiteSpace:"nowrap"}}>Status</th>
                                            <th style={{padding:"10px 14px",textAlign:"left",fontWeight:600,color:"var(--gray-2)",whiteSpace:"nowrap"}}>Sender</th>
                                            <th style={{padding:"10px 14px",textAlign:"left",fontWeight:600,color:"var(--gray-2)",whiteSpace:"nowrap"}}>Receiver</th>
                                            <th style={{padding:"10px 14px",textAlign:"left",fontWeight:600,color:"var(--gray-2)",whiteSpace:"nowrap"}}>Protocol</th>
                                            <th style={{padding:"10px 14px",textAlign:"left",fontWeight:600,color:"var(--gray-2)",whiteSpace:"nowrap"}}>Received At</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {(modalRcData.copies||[]).length === 0 ? (
                                            <tr><td colSpan={9} style={{padding:32,textAlign:"center",color:"var(--gray-3)",fontStyle:"italic"}}>No raw copies found for this reference</td></tr>
                                        ) : (modalRcData.copies||[]).map((row, ri) => (
                                            <React.Fragment key={row.id||ri}>
                                                <tr onClick={()=>setModalRcExpanded(p=>p===row.id?null:row.id)}
                                                    style={{background:ri%2===0?"var(--white)":"var(--gray-7)",cursor:"pointer",borderBottom:"1px solid var(--gray-6)"}}>
                                                    <td style={{padding:"8px 14px",textAlign:"center"}}>
                                                        <button style={{background:"none",border:"1px solid var(--gray-5)",borderRadius:4,width:20,height:20,cursor:"pointer",display:"flex",alignItems:"center",justifyContent:"center",transform:modalRcExpanded===row.id?"rotate(90deg)":"none",transition:"transform 0.15s"}}
                                                            onClick={e=>{e.stopPropagation();setModalRcExpanded(p=>p===row.id?null:row.id);}}>
                                                            <svg width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><polyline points="9 18 15 12 9 6"/></svg>
                                                        </button>
                                                    </td>
                                                    <td style={{padding:"8px 14px",fontFamily:"monospace",fontSize:11,color:"var(--black-3)"}}>{row.messageReference?row.messageReference.slice(0,22)+(row.messageReference.length>22?"…":""):"—"}</td>
                                                    <td style={{padding:"8px 14px"}}><span style={{fontFamily:"monospace",fontWeight:700,fontSize:12}}>{row.messageTypeCode||"—"}</span></td>
                                                    <td style={{padding:"8px 14px"}}><span className={rcDirCls(row.direction)}>{row.direction||"—"}</span></td>
                                                    <td style={{padding:"8px 14px"}}><span className={rcStatusCls(row.currentStatus)}>{row.currentStatus||"—"}</span></td>
                                                    <td style={{padding:"8px 14px",fontFamily:"monospace",fontSize:12}}>{row.senderAddress||"—"}</td>
                                                    <td style={{padding:"8px 14px",fontFamily:"monospace",fontSize:12}}>{row.receiverAddress||"—"}</td>
                                                    <td style={{padding:"8px 14px",fontSize:12}}>{row.protocol||"—"}</td>
                                                    <td style={{padding:"8px 14px",fontSize:12,whiteSpace:"nowrap"}}>{fmtDate(row.receivedAt||row.ampDateReceived)}</td>
                                                </tr>
                                                {modalRcExpanded===row.id&&(
                                                    <tr style={{background:"#f8fafc"}}>
                                                        <td colSpan={9} style={{padding:0}}>
                                                            <div style={{padding:"14px 18px",borderTop:"1px solid var(--gray-5)"}}>
                                                                <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:8}}>
                                                                    <div>
                                                                        <span style={{fontSize:11,fontWeight:700,color:"var(--gray-3)",letterSpacing:"0.06em",textTransform:"uppercase"}}>Raw Input — {row.inputType||"UNKNOWN"} · {row.source||"—"}</span>
                                                                        <div style={{fontSize:10,color:"var(--gray-4)",marginTop:2,fontFamily:"monospace"}}>ID: {row.id}</div>
                                                                    </div>
                                                                    {row.rawInput&&(
                                                                        <button onClick={e=>{e.stopPropagation();navigator.clipboard.writeText(row.rawInput||"").then(()=>{setModalRcCopied(row.id);setTimeout(()=>setModalRcCopied(null),1800);});}}
                                                                            style={{fontSize:11,fontWeight:600,padding:"4px 12px",borderRadius:6,border:"1px solid var(--accent)",background:modalRcCopied===row.id?"var(--accent)":"transparent",color:modalRcCopied===row.id?"white":"var(--accent)",cursor:"pointer",transition:"all 0.2s"}}>
                                                                            {modalRcCopied===row.id?"✓ Copied":"Copy XML"}
                                                                        </button>
                                                                    )}
                                                                </div>
                                                                {row.rawInput
                                                                    ?<pre style={{margin:0,padding:"12px 14px",background:"#0f172a",color:"#e2e8f0",borderRadius:8,fontSize:11,overflowX:"auto",fontFamily:"monospace",lineHeight:1.6,maxHeight:300,overflowY:"auto",whiteSpace:"pre-wrap",wordBreak:"break-all"}}>{row.rawInput}</pre>
                                                                    :<div style={{color:"var(--gray-3)",fontStyle:"italic",fontSize:13}}>No raw content available</div>
                                                                }
                                                            </div>
                                                        </td>
                                                    </tr>
                                                )}
                                            </React.Fragment>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        </>
                    )}
                    {!modalRcLoading && !modalRcError && !modalRcData && (
                        <div style={{padding:40,textAlign:"center",color:"var(--gray-3)"}}>
                            <p style={{margin:0,fontSize:13}}>No reference available for this message</p>
                        </div>
                    )}
                </div>}
            </div>
        </div>
    );
}



// ════════════════════════════════════════════════════════════════════════════════
// MAIN SEARCH COMPONENT
// ════════════════════════════════════════════════════════════════════════════════
function Search() {
    const { token } = useAuth();

    const authHeaders = useCallback(() => ({
        "Content-Type": "application/json",
        ...(token ? { "Authorization": `Bearer ${token}` } : {})
    }), [token]);

    const pagesPerGroup = 5;
    const [recordsPerPage, setRecordsPerPage] = useState(20);
    const [currentPage,  setCurrentPage]  = useState(1);
    const [startPage,    setStartPage]    = useState(1);
    const [showResult,   setShowResult]   = useState(false);
    const [goToPage,     setGoToPage]     = useState("");
    const [searchState,  setSearchState]  = useState(initialSearchState);
    const [result,       setResult]       = useState([]);
    const [allMessages,  setAllMessages]  = useState([]);
    const [isFetching,   setIsFetching]   = useState(false);
    const [fetchError,   setFetchError]   = useState(null);
    const [opts,         setOpts]         = useState(emptyOpts);
    const [optsLoading,  setOptsLoading]  = useState(true);
    const [activeCol,    setActiveCol]    = useState(null);
    const [colFilters,   setColFilters]   = useState({});
    const [sortKey,      setSortKey]      = useState(null);
    const [sortDir,      setSortDir]      = useState(SORT_NONE);
    const [selectedRows, setSelectedRows] = useState(new Set());
    const [visibleCols,  setVisibleCols]  = useState(new Set(COLUMNS.map(c=>c.key)));
    const [colWidths,    setColWidths]    = useState({});
    const colResizingRef = useRef(null);
    const [showColManager,  setShowColManager]  = useState(false);
    const [panelCollapsed,  setPanelCollapsed]  = useState(false);
    const [savedSearches,   setSavedSearches]   = useState([]);
    const [showSavedPanel,  setShowSavedPanel]  = useState(false);
    const [isSearching,     setIsSearching]     = useState(false);
    const [highlightText,   setHighlightText]   = useState("");
    const [showExportMenu,  setShowExportMenu]  = useState(false);
    const [exportScope,     setExportScope]     = useState("all");
    const [exportTargets,   setExportTargets]   = useState(["table"]);
    const [toastMsg,        setToastMsg]        = useState(null);
    const [openModals,      setOpenModals]      = useState([]);
    const openModalsRef = useRef([]);   // always-current mirror — no stale closure
    const topZRef  = useRef(1000);
    const modalIdRef = useRef(0);
    const [serverTotal,       setServerTotal]      = useState(0);
    const [serverTotalPages,  setServerTotalPages] = useState(0);
    const [isExporting,       setIsExporting]      = useState(false);
    const [searchMode,      setSearchMode]      = useState("fixed");
    const [advancedFields,  setAdvancedFields]  = useState([]);
    const [showFieldPicker, setShowFieldPicker] = useState(false);
    const [fieldPickerQuery,setFieldPickerQuery]= useState("");
    const [dynamicFields,   setDynamicFields]   = useState([]);
    const [dynFieldsLoaded, setDynFieldsLoaded] = useState(false);

    // ── Raw Copies state ─────────────────────────────────────────────────────
    const [rcFilters,        setRcFilters]        = useState(initialRcFilters);
    const [rcOpts,           setRcOpts]           = useState({ messageTypeCodes:[], directions:[], statuses:[], protocols:[], inputTypes:[], sources:[] });
    const [rcOptsLoading,    setRcOptsLoading]    = useState(true);
    const [rcResults,        setRcResults]        = useState([]);
    const [rcTotal,          setRcTotal]          = useState(0);
    const [rcTotalPages,     setRcTotalPages]     = useState(0);
    const [rcPage,           setRcPage]           = useState(0);
    // ── PATCHED: full pagination state (matches Fixed/Advanced) ──────────────
    const rcPagesPerGroup = 5;
    const [rcRecordsPerPage, setRcRecordsPerPage] = useState(20);
    const [rcGoToPage,       setRcGoToPage]       = useState("");
    const [rcStartPage,      setRcStartPage]      = useState(1);
    // ─────────────────────────────────────────────────────────────────────────
    const [rcLoading,        setRcLoading]        = useState(false);
    const [rcError,          setRcError]          = useState(null);
    const [rcSearched,       setRcSearched]       = useState(false);
    const [rcPanelCollapsed, setRcPanelCollapsed] = useState(false);
    const [rcExpandedRow,    setRcExpandedRow]    = useState(null);
    const [rcCopiedId,       setRcCopiedId]       = useState(null);

    const bottomScrollRef = useRef(null);
    const tableWrapperRef = useRef(null);
    const colManagerRef   = useRef(null);
    const exportMenuRef   = useRef(null);
    const fieldPickerRef  = useRef(null);

    const set      = (key) => (e) => setSearchState(s=>({...s,[key]:e.target.value}));
    const setField = (key,val) => setSearchState(s=>({...s,[key]:val}));
    const showToast = (msg,type="success") => { setToastMsg({msg,type}); setTimeout(()=>setToastMsg(null),3000); };

    // Keep openModalsRef in sync — no stale closure in openModal
    openModalsRef.current = openModals;

    // ── Auto-add new dynamic columns ────────────────────────────────────────
    useEffect(()=>{
        if (!dynFieldsLoaded) return;
        const newDynCols = getDynamicExtraColumns(dynamicFields);
        if (newDynCols.length > 0) {
            setVisibleCols(prev => {
                const next = new Set(prev);
                newDynCols.forEach(f => next.add(f.key));
                return next;
            });
        }
    }, [dynFieldsLoaded, dynamicFields]);

    // ── Load dynamic field config ────────────────────────────────────────────
    useEffect(()=>{
        if (!token) return;
        fetch(API_FIELD_CFG_URL, { headers: authHeaders() })
            .then(r=>{ if(!r.ok) throw new Error("field-config error"); return r.json(); })
            .then(data=>{
                const convertedMap = new Map();
                data.forEach(f => {
                    const normalized = normalizeDynamicField(f);
                    const existing = convertedMap.get(normalized.key);
                    if (!existing) {
                        convertedMap.set(normalized.key, normalized);
                        return;
                    }
                    convertedMap.set(normalized.key, {
                        ...existing,
                        _backendOpts: existing._backendOpts?.length ? existing._backendOpts : normalized._backendOpts,
                        columnLabel: existing.columnLabel || normalized.columnLabel,
                        showInTable: existing.showInTable || normalized.showInTable,
                    });
                });
                const converted = [...convertedMap.values()];
                setDynamicFields(converted);
                setDynFieldsLoaded(true);
                const newKeys = converted.filter(f=>!(f.key in initialSearchState));
                if (newKeys.length > 0) {
                    const patch = {};
                    newKeys.forEach(f => { patch[f.key] = ""; });
                    setSearchState(s=>({...s,...patch}));
                }
            })
            .catch(()=>{ setDynFieldsLoaded(true); });
    // eslint-disable-next-line react-hooks/exhaustive-deps
    },[token]);

    // ── Load dropdown options ────────────────────────────────────────────────
    useEffect(()=>{
        if (!token) return;
        setOptsLoading(true);
        fetch(API_DROPDOWN_URL, { headers: authHeaders() })
            .then(r=>{ if(!r.ok) throw new Error("dropdown-options error"); return r.json(); })
            .then(data=>{
                if(data.allMtMxTypes) allMtMxTypeMap = buildAllMtMxTypeMap(data.allMtMxTypes);
                setOpts(prev=>({
                    ...prev, ...data,
                    formats:              data.formats               || ["MT","MX"],
                    types:                data.messageCodes          || data.types              || [],
                    mtTypes:              data.mtTypes               || (data.messageCodes||[]).filter(c=>c.toUpperCase().startsWith("MT")).sort(),
                    mxTypes:              data.mxTypes               || (data.messageCodes||[]).filter(c=>!c.toUpperCase().startsWith("MT")).sort(),
                    allMtMxTypes:         data.allMtMxTypes          || [],
                    messageCodes:         data.messageCodes          || [],
                    directions:           data.ioDirections          || data.directions         || [],
                    statuses:             data.statuses              || [],
                    actions:              data.actions               || [],
                    phases:               data.phases                || [],
                    reasons:              data.reasons               || [],
                    ownerUnits:           data.owners                || data.ownerUnits          || [],
                    backendChannels:      data.networkChannels       || data.backendChannels     || [],
                    networkChannels:      data.networkChannels       || [],
                    networks:             data.networkProtocols      || data.networks            || [],
                    networkPriorities:    data.networkPriorities     || [],
                    deliveryModes:        data.deliveryModes         || [],
                    services:             data.services              || [],
                    currencies:           data.currencies            || [],
                    sourceSystems:        data.sourceSystems         || [],
                    countries:            data.countries             || [],
                    workflows:            data.workflows             || [],
                    workflowModels:       data.workflowModels        || [],
                    originatorApplications:data.originatorApplications||[],
                    finCopies:            data.finCopies             || [],
                    finCopyServices:      data.finCopyServices       || [],
                    processingTypes:      data.processingTypes       || [],
                    processPriorities:    data.processPriorities     || [],
                    profileCodes:         data.profileCodes          || [],
                    amlStatuses:          data.amlStatuses           || [],
                    nackCodes:            data.nackCodes             || [],
                    messagePriorities:    data.messagePriorities     || [],
                    copyIndicators:       data.copyIndicators        || [],
                }));
                setOptsLoading(false);
            })
            .catch(()=>{ setOptsLoading(false); });
    // eslint-disable-next-line react-hooks/exhaustive-deps
    },[token]);

    // ── Load Raw Copies dropdown options ─────────────────────────────────────
    useEffect(()=>{
        if (!token) return;
        fetch(`${API_RAW_COPIES_URL}/dropdown-options`, { headers: authHeaders() })
            .then(r => r.json())
            .then(d => { setRcOpts(d.data || d); setRcOptsLoading(false); })
            .catch(() => setRcOptsLoading(false));
    // eslint-disable-next-line react-hooks/exhaustive-deps
    },[token]);

    // Derive opts from loaded messages when API opts are empty
    useEffect(()=>{
        if(optsLoading || opts.formats.length>0 || allMessages.length===0) return;
        const unique = (key) => [...new Set(allMessages.map(m=>m[key]).filter(Boolean))].sort();
        const typesFromMt      = [...new Set(allMessages.filter(m=>normalizeFormat(m.format)==="MT").map(m=>m.type).filter(t=>t&&!t.includes("/")))].sort();
        const typesFromMx      = [...new Set(allMessages.filter(m=>normalizeFormat(m.format)==="MX").map(m=>m.type).filter(t=>t&&!t.includes("/")))].sort();
        const typesFromAllMtMx = [...new Set(allMessages.filter(m=>normalizeFormat(m.format)==="ALL MT&MX").map(m=>m.type).filter(t=>t&&!t.includes("/")))].sort();
        const allMtMxPairs = Object.entries(allMtMxTypeMap).filter(([,sides])=>typesFromAllMtMx.some(t=>sides.includes(t))).map(([k])=>k);
        allMtMxTypeMap = buildAllMtMxTypeMap(allMtMxPairs);
        const allIndividual = [...new Set([...typesFromMt,...typesFromMx,...typesFromAllMtMx])].sort();
        const formats=[];
        if(typesFromMt.length)   formats.push("MT");
        if(typesFromMx.length)   formats.push("MX");
        if(allMtMxPairs.length)  formats.push("ALL MT&MX");
        setOpts({ formats, types:allIndividual, mtTypes:typesFromMt, mxTypes:typesFromMx, allMtMxTypes:allMtMxPairs, networks:unique("network"), sourceSystems:unique("sourceSystem"), currencies:unique("currency"), ownerUnits:unique("ownerUnit"), backendChannels:unique("backendChannel"), directions:unique("direction"), statuses:unique("status"), finCopies:unique("finCopy"), actions:unique("action"), phases:unique("phase"), messageCodes:allIndividual, countries:unique("country"), workflows:unique("workflow"), networkChannels:unique("backendChannel"), networkPriorities:[], senders:unique("sender"), receivers:unique("receiver") });
    },[allMessages, optsLoading, opts.formats.length]);

    const typeOptions = useMemo(()=>{
        if(searchState.format==="MT")        return opts.mtTypes      || [];
        if(searchState.format==="MX")        return opts.mxTypes      || [];
        if(searchState.format==="ALL MT&MX") return opts.allMtMxTypes || [];
        return opts.types || [];
    },[searchState.format, opts]);

    const activeFieldDefs = useMemo(()=>{
        const visible = (fields) => fields.filter(f => !ADV_HIDDEN_GROUPS.has(f.group) && isAdvancedFixedField(f));
        if (dynFieldsLoaded && dynamicFields.length > 0) {
            const dynKeys = new Set(dynamicFields.map(f=>f.key));
            const staticOnly = FIELD_DEFINITIONS.filter(f=>!dynKeys.has(f.key));
            return visible([...dynamicFields, ...staticOnly]);
        }
        return visible(FIELD_DEFINITIONS);
    }, [dynFieldsLoaded, dynamicFields]);

    useEffect(() => {
        const allowedKeys = new Set(activeFieldDefs.map(f => f.key));
        setAdvancedFields(prev => prev.filter(key => allowedKeys.has(key)));
    }, [activeFieldDefs]);

    // Close dropdowns on outside click
    useEffect(()=>{
        const h=(e)=>{
            if(colManagerRef.current&&!colManagerRef.current.contains(e.target)) setShowColManager(false);
            if(exportMenuRef.current&&!exportMenuRef.current.contains(e.target))  setShowExportMenu(false);
            if(fieldPickerRef.current&&!fieldPickerRef.current.contains(e.target)) setShowFieldPicker(false);
        };
        document.addEventListener("mousedown",h);
        return ()=>document.removeEventListener("mousedown",h);
    },[]);

    useEffect(()=>{
        const onKey=(e)=>{ if(e.key==="Escape"){ setShowFieldPicker(false); } };
        document.addEventListener("keydown",onKey);
        return ()=>document.removeEventListener("keydown",onKey);
    });

    const syncScroll=(src)=>{
        const sl=src.currentTarget.scrollLeft;
        if(src.currentTarget!==bottomScrollRef.current&&bottomScrollRef.current) bottomScrollRef.current.scrollLeft=sl;
        if(src.currentTarget!==tableWrapperRef.current&&tableWrapperRef.current) tableWrapperRef.current.scrollLeft=sl;
    };

    // ── Mode switch ──────────────────────────────────────────────────────────
    const handleModeSwitch = (mode) => {
        if (mode === searchMode) return;
        setSearchMode(mode);
        if (mode !== "rawcopies") {
            setSearchState(initialSearchState);
            setResult([]); setShowResult(false);
            setCurrentPage(1); setStartPage(1);
            setColFilters({}); setActiveCol(null);
            setSortKey(null); setSortDir(SORT_NONE);
            setSelectedRows(new Set()); setHighlightText("");
            setServerTotal(0); setServerTotalPages(0);
            if (mode === "advanced") setAdvancedFields(["dateRange"]);
        }
        showToast(`Switched to ${mode === "fixed" ? "Fixed" : mode === "advanced" ? "Advanced" : "Raw Copies"} Search`, "info");
    };

    const addAdvancedField = (fieldKey) => {
        if (!advancedFields.includes(fieldKey)) setAdvancedFields(p=>[...p, fieldKey]);
        setShowFieldPicker(false);
        setFieldPickerQuery("");
    };

    const removeAdvancedField = (fieldKey) => {
        setAdvancedFields(p=>p.filter(k=>k!==fieldKey));
        const def = activeFieldDefs.find(f=>f.key===fieldKey) || FIELD_DEFINITIONS.find(f=>f.key===fieldKey);
        if (def) {
            const patch = {};
            (def.stateKeys||[fieldKey]).forEach(sk=>{ patch[sk]=""; });
            setSearchState(s=>({...s,...patch}));
        }
    };

    const advancedResultCols = useMemo(()=>{
        if (searchMode !== "advanced") return null;
        const colKeySet = new Set(ADV_BASE_COLS);
        const selectedDefs = advancedFields
            .map(fkey => activeFieldDefs.find(f=>f.key===fkey) || FIELD_DEFINITIONS.find(f=>f.key===fkey))
            .filter(Boolean);
        selectedDefs.forEach(def => def.colKeys.forEach(ck=>colKeySet.add(ck)));
        const extraCols = getDynamicExtraColumns(selectedDefs);
        const staticFiltered = COLUMNS.filter(c=>colKeySet.has(c.key));
        const derivedCols = buildAdvancedDerivedColumns(selectedDefs, result);
        return [
            ...staticFiltered,
            ...extraCols.filter(c=>!staticFiltered.find(s=>s.key===c.key)),
            ...derivedCols.filter(c=>!staticFiltered.find(s=>s.key===c.key) && !extraCols.find(e=>e.key===c.key)),
        ];
    },[searchMode, advancedFields, activeFieldDefs, result]);

    const handleClear=()=>{
        setSearchState(initialSearchState); setResult([]); setShowResult(false);
        setCurrentPage(1); setStartPage(1); setColFilters({}); setActiveCol(null);
        setGoToPage(""); setSortKey(null); setSortDir(SORT_NONE);
        setSelectedRows(new Set()); setHighlightText(""); setExportScope("all"); setExportTargets(["table"]);
        setServerTotal(0); setServerTotalPages(0);
        if (searchMode==="advanced") setAdvancedFields(["dateRange"]);
    };

    // ── Build URL params ─────────────────────────────────────────────────────
    const buildSearchRequest = useCallback((s, page, size) => {
        const filters = {};
        const d = (v) => v && v.replace(/\//g, "-");
        const setFilter = (key, value) => {
            if (value !== undefined && value !== null && value !== "") {
                filters[key] = String(value);
            }
        };

        setFilter("messageType", s.format);
        const msgCode = s.messageCode || s.type;
        setFilter("messageCode", msgCode);
        const dirVal = s.direction || s.io;
        setFilter("io", dirVal);
        setFilter("status", s.status);
        setFilter("messagePriority", s.messagePriority);
        setFilter("copyIndicator", s.copyIndicator);
        setFilter("finCopyService", s.finCopy);
        setFilter("possibleDuplicate", s.possibleDuplicate);
        setFilter("sender", s.sender);
        setFilter("receiver", s.receiver);
        setFilter("correspondent", s.correspondent);
        setFilter("reference", s.reference || s.messageReference);
        setFilter("transactionReference", s.transactionReference);
        setFilter("transferReference", s.transferReference);
        setFilter("relatedReference", s.relatedReference || s.rfkReference);
        setFilter("mur", s.userReference);
        setFilter("uetr", s.uetr);
        setFilter("mxInputReference", s.mxInputReference);
        setFilter("mxOutputReference", s.mxOutputReference);
        setFilter("networkReference", s.networkReference);
        setFilter("e2eMessageId", s.e2eMessageId);
        setFilter("ccy", s.currency);
        if(s.amountFrom && !isNaN(parseFloat(s.amountFrom))) setFilter("amountFrom", parseFloat(s.amountFrom));
        if(s.amountTo   && !isNaN(parseFloat(s.amountTo)))   setFilter("amountTo", parseFloat(s.amountTo));
        setFilter("networkProtocol", s.network);
        const netChan = s.backendChannel || s.networkChannel;
        setFilter("networkChannel", netChan);
        setFilter("networkPriority", s.networkPriority);
        setFilter("deliveryMode", s.deliveryMode);
        setFilter("service", s.service);
        setFilter("country", s.country);
        setFilter("originCountry", s.originCountry);
        setFilter("destinationCountry", s.destinationCountry);
        setFilter("owner", s.ownerUnit);
        setFilter("workflow", s.workflow);
        setFilter("workflowModel", s.workflowModel);
        setFilter("originatorApplication", s.originatorApplication);
        setFilter("sourceSystem", s.sourceSystem);
        setFilter("phase", s.phase);
        setFilter("action", s.action);
        setFilter("reason", s.reason);
        setFilter("processingType", s.processingType);
        setFilter("processPriority", s.processPriority);
        setFilter("profileCode", s.profileCode);
        setFilter("environment", s.environment);
        setFilter("nack", s.nack);
        setFilter("amlStatus", s.amlStatus);
        setFilter("amlDetails", s.amlDetails);
        if(s.seqFrom && !isNaN(parseInt(s.seqFrom,10))) setFilter("seqFrom", parseInt(s.seqFrom,10));
        if(s.seqTo   && !isNaN(parseInt(s.seqTo,10)))   setFilter("seqTo", parseInt(s.seqTo,10));
        setFilter("sessionNumber", s.sessionNumber);
        setFilter("logicalTerminalAddress", s.logicalTerminalAddress);
        setFilter("startDate", d(s.startDate));
        setFilter("endDate", d(s.endDate));
        setFilter("valueDateFrom", d(s.valueDateFrom));
        setFilter("valueDateTo", d(s.valueDateTo));
        setFilter("statusDateFrom", d(s.statusDateFrom));
        setFilter("statusDateTo", d(s.statusDateTo));
        setFilter("receivedDateFrom", d(s.receivedDateFrom));
        setFilter("receivedDateTo", d(s.receivedDateTo));
        setFilter("historyEntity", s.historyEntity);
        setFilter("historyDescription", s.historyDescription);
        setFilter("historyPhase", s.historyPhase);
        setFilter("historyAction", s.historyAction);
        setFilter("historyUser", s.historyUser);
        setFilter("historyChannel", s.historyChannel);
        setFilter("freeSearchText", s.freeSearchText);

        const handledBackendParams = new Set([
            "messageType", "messageCode", "io", "status", "messagePriority", "copyIndicator",
            "finCopyService", "possibleDuplicate", "sender", "receiver", "correspondent",
            "reference", "transactionReference", "transferReference", "relatedReference", "mur",
            "uetr", "mxInputReference", "mxOutputReference", "networkReference", "e2eMessageId",
            "ccy", "amountFrom", "amountTo", "networkProtocol", "networkChannel", "networkPriority",
            "deliveryMode", "service", "country", "originCountry", "destinationCountry", "owner",
            "workflow", "workflowModel", "originatorApplication", "sourceSystem", "phase", "action",
            "reason", "processingType", "processPriority", "profileCode", "environment", "nack",
            "amlStatus", "amlDetails", "seqFrom", "seqTo", "sessionNumber", "startDate", "endDate", "valueDateFrom",
            "valueDateTo", "statusDateFrom", "statusDateTo", "receivedDateFrom", "receivedDateTo",
            "historyEntity", "historyDescription", "historyPhase", "historyAction", "historyUser",
            "historyChannel", "freeSearchText",
        ]);

        activeFieldDefs.forEach(def => {
            const backendParam = typeof def?.backendParam === "string" ? def.backendParam : "";
            const stateKeys = Array.isArray(def?.stateKeys) && def.stateKeys.length ? def.stateKeys : [def?.key];
            if (!backendParam || handledBackendParams.has(backendParam) || backendParam.includes(",") || stateKeys.length !== 1) {
                return;
            }
            if (def?.type === "date-range" || def?.type === "date-range2" || def?.type === "amount-range" || def?.type === "seq-range") {
                return;
            }
            const value = s[stateKeys[0]];
            if (value !== undefined && value !== null && value !== "") {
                setFilter(backendParam, value);
            }
        });

        return {
            page,
            size,
            filters,
        };
    }, [activeFieldDefs]);

    // ── Execute SWIFT message search ──────────────────────────────────────────
    const isEventArg = (value) => !!(value && typeof value === "object" && (
        typeof value.preventDefault === "function" ||
        "nativeEvent" in value ||
        "currentTarget" in value ||
        "target" in value
    ));

    const handleSearch=(pageOverride, sizeOverride)=>{
        setIsSearching(true); setIsFetching(true); setFetchError(null);
        try {
            const safePageOverride = isEventArg(pageOverride) ? undefined : pageOverride;
            const safeSizeOverride = isEventArg(sizeOverride) ? undefined : sizeOverride;
            const page = (safePageOverride !== undefined) ? safePageOverride : 0;
            const size = safeSizeOverride ?? recordsPerPage;
            const requestBody = buildSearchRequest(searchState, page, size);
            fetch(API_BASE_URL, { method: "POST", headers: authHeaders(), body: JSON.stringify(requestBody) })
            .then(r=>{ if(!r.ok) throw new Error(`Search failed (${r.status})`); return r.json(); })
            .then(data=>{
                const rows = data.content || data;
                setResult(rows); setAllMessages(rows);
                setServerTotal(data.totalElements || rows.length);
                setServerTotalPages(data.totalPages || 1);
                setCurrentPage((data.pageNumber||0)+1);
                setStartPage(Math.floor((data.pageNumber||0)/pagesPerGroup)*pagesPerGroup+1);
                setHighlightText(searchState.freeSearchText||"");
                setShowResult(true); setColFilters({}); setActiveCol(null); setGoToPage("");
                setSortKey(null); setSortDir(SORT_NONE);
                setSelectedRows(new Set()); setExportScope("all"); setExportTargets(["table"]);
                setIsSearching(false); setIsFetching(false);
                const total = data.totalElements || rows.length;
                showToast(`Found ${total} message${total!==1?"s":""}`, "info");
                if(!panelCollapsed && total>0) setPanelCollapsed(true);
            })
            .catch(err=>{
                setFetchError(err.message); setIsSearching(false); setIsFetching(false);
                showToast(err.message, "error");
            });
        } catch (err) {
            const message = err?.message || "Search request build failed";
            setFetchError(message); setIsSearching(false); setIsFetching(false);
            showToast(message, "error");
        }
    };

    const handleKeyDown=(e)=>{ if(e.key==="Enter") handleSearch(); };

    // ── Raw Copies search ────────────────────────────────────────────────────
    const setRcField = (key) => (e) => setRcFilters(f => ({ ...f, [key]: e.target.value }));

    const doRcSearch = useCallback((p = 0, sizeOverride) => {
        if (!token) return;
        setRcLoading(true); setRcError(null);
        const size = sizeOverride ?? rcRecordsPerPage;
        const params = new URLSearchParams({ page: p, size });
        Object.entries(rcFilters).forEach(([k, v]) => { if (v !== "") params.set(k, v); });
        fetch(`${API_RAW_COPIES_URL}?${params}`, { headers: authHeaders() })
            .then(r => { if (!r.ok) throw new Error(`Error ${r.status}`); return r.json(); })
            .then(data => {
                const rows = data.content || data.rows || [];
                setRcResults(rows);
                setRcTotal(data.totalElements || rows.length);
                setRcTotalPages(data.totalPages || 1);
                setRcPage(p);
                // PATCHED: update rcStartPage
                setRcStartPage(Math.floor(p / rcPagesPerGroup) * rcPagesPerGroup + 1);
                setRcSearched(true);
                setRcLoading(false);
                if (!rcPanelCollapsed && rows.length > 0) setRcPanelCollapsed(true);
                showToast(`Found ${data.totalElements || rows.length} raw cop${(data.totalElements || rows.length) !== 1 ? "ies" : "y"}`, "info");
            })
            .catch(e => { setRcError(e.message); setRcLoading(false); showToast(e.message, "error"); });
    // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [token, authHeaders, rcFilters, rcPanelCollapsed, rcRecordsPerPage]);

    const handleRcReset = () => {
        setRcFilters(initialRcFilters);
        setRcResults([]); setRcTotal(0); setRcTotalPages(0); setRcPage(0);
        // PATCHED: reset new pagination state
        setRcStartPage(1); setRcGoToPage("");
        setRcSearched(false); setRcExpandedRow(null); setRcError(null); setRcPanelCollapsed(false);
    };

    const handleRcKey = (e) => { if (e.key === "Enter") doRcSearch(0); };

    const rcGrouped = useMemo(() => {
        const map = new Map();
        rcResults.forEach(row => {
            const ref = row.messageReference || "—";
            if (!map.has(ref)) map.set(ref, []);
            map.get(ref).push(row);
        });
        return [...map.entries()];
    }, [rcResults]);

    const copyRaw = (id, text) => {
        navigator.clipboard.writeText(text || "").then(() => {
            setRcCopiedId(id);
            setTimeout(() => setRcCopiedId(null), 1800);
        });
    };

    // ── Reference helper ─────────────────────────────────────────────────────
    const getReference=(msg)=>
        msg.reference || msg.mur || msg.transactionReference || msg.transferReference ||
        msg.relatedReference || msg.userReference || msg.rfkReference || msg.messageReference ||
        (msg.uetr ? `UETR-${String(msg.uetr).slice(0,8).toUpperCase()}` : null) ||
        `ID-${String(msg.id||msg.sequenceNumber||"").slice(0,10)||"UNKNOWN"}`;

    const saveSearch=()=>{ const name=prompt("Name this search:"); if(!name)return; setSavedSearches(p=>[...p,{name,state:{...searchState},mode:searchMode,advFields:[...advancedFields],ts:Date.now()}]); showToast(`Search "${name}" saved`); };
    const loadSearch=(s)=>{ setSearchState(s.state); if(s.mode) setSearchMode(s.mode); if(s.advFields) setAdvancedFields(s.advFields); setShowSavedPanel(false); showToast(`Loaded "${s.name}"`); };
    const deleteSearch=(idx)=>setSavedSearches(p=>p.filter((_,i)=>i!==idx));

    const handleSort=(key)=>{
        if(sortKey!==key){ setSortKey(key); setSortDir(SORT_ASC); }
        else if(sortDir===SORT_NONE||sortDir===null){ setSortDir(SORT_ASC); }
        else if(sortDir===SORT_ASC){ setSortDir(SORT_DESC); }
        else { setSortKey(null); setSortDir(SORT_NONE); }
        setCurrentPage(1); setStartPage(1);
    };

    const handleColFilter=(key,value)=>{ setColFilters(p=>({...p,[key]:value})); setCurrentPage(1); setStartPage(1); };

    // Stable unique key — MongoDB _id priority, no Math.random()
    const getMsgId = (msg) => {
        if (msg.id !== undefined && msg.id !== null && msg.id !== "") return String(msg.id);

        const raw = msg.rawMessage || msg;
        const rawId = raw?._id;
        if (rawId?.$oid) return String(rawId.$oid);
        if (typeof rawId === "string" || typeof rawId === "number") return String(rawId);
        if (msg.transactionReference) return `txn-${msg.transactionReference}`;
        if (msg.reference)            return `ref-${msg.reference}`;
        if (msg.mur)                  return `mur-${msg.mur}`;
        return `seq-${msg.sequenceNumber}-${msg.creationDate || msg.date || ""}`;
    };

    const toggleRow=(id)=>setSelectedRows(p=>{ const n=new Set(p); n.has(id)?n.delete(id):n.add(id); return n; });
    const toggleCol=(key)=>setVisibleCols(p=>{
        const n=new Set(p);
        if(n.has(key)){
            if(searchMode!=="fixed"&&n.size<=3){ showToast("Minimum 3 columns required","error"); return p; }
            n.delete(key);
        } else n.add(key);
        return n;
    });

    const handleColResizeStart = useCallback((e, colKey, thEl) => {
        e.preventDefault(); e.stopPropagation();
        const startW = thEl.offsetWidth;
        colResizingRef.current = { key: colKey, startX: e.clientX, startW, thEl };
        document.body.style.userSelect = "none";
        document.body.style.cursor = "col-resize";
        const onMove = (ev) => {
            const r = colResizingRef.current;
            if (!r) return;
            const newW = Math.max(60, r.startW + (ev.clientX - r.startX));
            r.thEl.style.width    = newW + "px";
            r.thEl.style.minWidth = newW + "px";
        };
        const onUp = (ev) => {
            const r = colResizingRef.current;
            if (!r) return;
            const newW = Math.max(60, r.startW + (ev.clientX - r.startX));
            colResizingRef.current = null;
            document.body.style.userSelect = "";
            document.body.style.cursor = "";
            setColWidths(prev => ({ ...prev, [r.key]: newW }));
            window.removeEventListener("mousemove", onMove);
            window.removeEventListener("mouseup",   onUp);
        };
        window.addEventListener("mousemove", onMove);
        window.addEventListener("mouseup",   onUp);
    }, []);

    const resetColWidth = useCallback((colKey) => {
        setColWidths(prev => { const n = {...prev}; delete n[colKey]; return n; });
    }, []);

    const ALWAYS_VISIBLE_COLS = new Set(["sequenceNumber","sessionNumber","format","type","date","time"]);

    const allColumns = useMemo(()=>{
        if (searchMode === "fixed") return FIXED_TABLE_COLUMNS;
        if (!dynFieldsLoaded || dynamicFields.length === 0) return COLUMNS;
        return [...COLUMNS, ...getDynamicExtraColumns(dynamicFields)];
    }, [searchMode, dynFieldsLoaded, dynamicFields]);

    const autoVisibleCols = useMemo(()=>{
        if (searchMode !== "fixed" || result.length === 0) return new Set(allColumns.map(c=>c.key));
        const hasValue = new Set();
        result.forEach(msg => {
            allColumns.forEach(col => {
                const v = msg[col.key];
                if (v !== null && v !== undefined && v !== "" && v !== false) hasValue.add(col.key);
            });
        });
        ALWAYS_VISIBLE_COLS.forEach(k => hasValue.add(k));
        return hasValue;
    }, [result, searchMode, allColumns]);

    const shownCols = searchMode==="advanced" && advancedResultCols
        ? advancedResultCols
        : allColumns.filter(c => visibleCols.has(c.key) && autoVisibleCols.has(c.key));
    const fixedColumnKeys = allColumns.map(c=>c.key);
    const selectedFixedColumnCount = fixedColumnKeys.filter(key=>visibleCols.has(key)).length;
    const selectAllFixedColumns = () => setVisibleCols(new Set(fixedColumnKeys));
    const deselectAllFixedColumns = () => setVisibleCols(new Set());

    const processed=useMemo(()=>{
        let data=result.filter(msg=>allColumns.every(col=>{ const fv=colFilters[col.key]; if(!fv)return true; if(col.key==="format")return getDisplayFormat(msg).toLowerCase().includes(fv.toLowerCase()); if(col.key==="type")return getDisplayType(msg).toLowerCase().includes(fv.toLowerCase()); return String(msg[col.key]??"").toLowerCase().includes(fv.toLowerCase()); }));
        if(sortKey&&sortDir!==SORT_NONE){
            data=[...data].sort((a,b)=>{
                const av=sortKey==="format"?getDisplayFormat(a):sortKey==="type"?getDisplayType(a):(a[sortKey]??"");
                const bv=sortKey==="format"?getDisplayFormat(b):sortKey==="type"?getDisplayType(b):(b[sortKey]??"");
                const cmp=typeof av==="number"?av-bv:String(av).localeCompare(String(bv));
                return sortDir===SORT_ASC?cmp:-cmp;
            });
        }
        return data;
    },[result,colFilters,sortKey,sortDir]);

    const indexOfLast=currentPage*recordsPerPage, indexOfFirst=indexOfLast-recordsPerPage;
    const currentRecords=processed;
    const totalPages=serverTotalPages || Math.ceil(processed.length/recordsPerPage);

    const handlePageClick=(page)=>{
        setCurrentPage(page);
        setSelectedRows(new Set());
        setStartPage(Math.floor((page-1)/pagesPerGroup)*pagesPerGroup+1);
        handleSearch(page-1);
    };

    // ── Export ───────────────────────────────────────────────────────────────
    const fetchSearchPage = useCallback(async (page, size) => {
        const requestBody = buildSearchRequest(searchState, page, size);
        const res = await fetch(API_BASE_URL, {
            method: "POST",
            headers: authHeaders(),
            body: JSON.stringify(requestBody),
        });
        if (!res.ok) throw new Error(`Export fetch failed (${res.status}) on page ${page + 1}`);
        const data = await res.json();
        return {
            rows: Array.isArray(data?.content) ? data.content : (Array.isArray(data) ? data : []),
            totalPages: Number(data?.totalPages) || 1,
            pageSize: Number(data?.pageSize) || size,
        };
    }, [authHeaders, buildSearchRequest, searchState]);

    const fetchAllRows = useCallback(async () => {
        const requestBody = buildSearchRequest(searchState, 0, recordsPerPage);
        const res = await fetch(API_EXPORT_ALL_URL, {
            method: "POST",
            headers: authHeaders(),
            body: JSON.stringify({ filters: requestBody.filters }),
        });
        if (!res.ok) throw new Error(`Export fetch failed (${res.status})`);
        const data = await res.json();
        return Array.isArray(data) ? data : (Array.isArray(data?.content) ? data.content : []);
    }, [authHeaders, buildSearchRequest, searchState, recordsPerPage]);

    const backendTableExportColumns = useMemo(() => (
        [{ key: "reference", label: "Reference" }, ...shownCols.map(c => ({ key: c.key, label: c.label }))]
    ), [shownCols]);

    const downloadBackendTableAllExport = useCallback(async (format) => {
        const requestBody = buildSearchRequest(searchState, 0, recordsPerPage);
        const res = await fetch(`${API_EXPORT_ALL_FILE_URL}?format=${encodeURIComponent(format)}`, {
            method: "POST",
            headers: authHeaders(),
            body: JSON.stringify({
                filters: requestBody.filters,
                columns: backendTableExportColumns,
            }),
        });
        if (!res.ok) {
            let detail = "";
            try { detail = await res.text(); } catch {}
            throw new Error(`Export download failed (${res.status})${detail ? `: ${detail}` : ""}`);
        }
        const blob = await res.blob();
        const filename = extractFilenameFromDisposition(res.headers.get("Content-Disposition"))
            || `swift_messages_result_table_all.${format === "excel" ? "xlsx" : format}`;
        triggerDownload(blob, filename);
    }, [authHeaders, backendTableExportColumns, buildSearchRequest, recordsPerPage, searchState]);

    const fetchMessageDetailsByReferences = useCallback(async (references) => {
        const uniqueRefs = [...new Set((references || []).filter(Boolean))];
        if (uniqueRefs.length === 0) return new Map();
        const res = await fetch(API_DETAILS_BY_REFS_URL, {
            method: "POST",
            headers: authHeaders(),
            body: JSON.stringify(uniqueRefs),
        });
        if (!res.ok) throw new Error(`Detail fetch failed (${res.status})`);
        const data = await res.json();
        const rows = Array.isArray(data) ? data : [];
        return new Map(rows.map(row => [row.reference || row.messageReference, row]));
    }, [authHeaders]);

    const ensureDetailedMessages = useCallback(async (messages, targetKeys) => {
        const orderedKeys = Array.isArray(targetKeys) && targetKeys.length ? targetKeys : ["table"];
        if (orderedKeys.length === 1 && orderedKeys[0] === "table") {
            return messages || [];
        }
        const refsNeedingDetail = (messages || [])
            .filter(msg => !msg.rawMessage)
            .map(msg => msg.reference || msg.messageReference)
            .filter(Boolean);
        if (refsNeedingDetail.length === 0) {
            return messages || [];
        }
        const detailMap = await fetchMessageDetailsByReferences(refsNeedingDetail);
        return (messages || []).map(msg => detailMap.get(msg.reference || msg.messageReference) || msg);
    }, [fetchMessageDetailsByReferences]);

    const getExportRows = (scope) => {
        if (scope === "selected") return processed.filter(m => selectedRows.has(getMsgId(m)));
        if (scope === "page")     return currentRecords;
        return null;
    };

    const createBackendExportJob = useCallback(async ({ format, scope, targetKeys }) => {
        const orderedKeys = Array.isArray(targetKeys) && targetKeys.length ? targetKeys : ["table"];
        const isTableOnly = orderedKeys.length === 1 && orderedKeys[0] === "table";
        const payload = {
            format,
            scope,
            targetKeys: orderedKeys,
            columns: isTableOnly ? backendTableExportColumns : [],
        };

        if (scope === "all") {
            const requestBody = buildSearchRequest(searchState, 0, recordsPerPage);
            payload.filters = requestBody.filters;
            payload.totalCount = serverTotal;
        } else {
            const references = [...new Set((getExportRows(scope) || []).map(getReference).filter(Boolean))];
            if (!references.length) {
                throw new Error("No messages available for export.");
            }
            payload.references = references;
            payload.totalCount = references.length;
        }

        const res = await fetch(API_EXPORT_JOBS_URL, {
            method: "POST",
            headers: authHeaders(),
            body: JSON.stringify(payload),
        });
        if (!res.ok) {
            let detail = "";
            try { detail = await res.text(); } catch {}
            throw new Error(`Export job creation failed (${res.status})${detail ? `: ${detail}` : ""}`);
        }
        return res.json();
    }, [authHeaders, backendTableExportColumns, buildSearchRequest, currentRecords, getReference, processed, recordsPerPage, searchState, selectedRows, serverTotal]);

    const fetchRawCopiesByRefs = useCallback(async (references) => {
        const uniqueRefs = [...new Set((references || []).filter(Boolean))];
        if (uniqueRefs.length === 0) return new Map();

        const grouped = new Map(uniqueRefs.map(ref => [ref, []]));
        const batchSize = 100;
        for (let i = 0; i < uniqueRefs.length; i += batchSize) {
            const batch = uniqueRefs.slice(i, i + batchSize);
            const res = await fetch(`${API_RAW_COPIES_URL}/by-refs`, {
                method: "POST",
                headers: authHeaders(),
                body: JSON.stringify(batch),
            });
            if (!res.ok) throw new Error(`Raw copies export fetch failed (${res.status})`);
            const data = await res.json();
            const payload = data?.data || data || {};
            Object.entries(payload).forEach(([ref, copies]) => {
                grouped.set(ref, Array.isArray(copies) ? copies : []);
            });
        }
        return grouped;
    }, [authHeaders]);

    const buildResultTableDataset = useCallback((messages) => {
        const columns = [{ key: "reference", label: "Reference" }, ...shownCols.map(c => ({ key: c.key, label: c.label }))];
        const rows = (messages || []).map(msg => {
            const out = { reference: getReference(msg) };
            shownCols.forEach(c => {
                out[c.key] = c.key === "format"
                    ? getDisplayFormat(msg)
                    : c.key === "type"
                        ? getDisplayType(msg)
                        : (msg[c.key] != null ? msg[c.key] : "");
            });
            return out;
        });
        return { label: "Result Table", columns, rows };
    }, [shownCols, getReference]);

    const getExportMessageMeta = useCallback((msg) => ({
        messageReference: msg.reference || msg.messageReference || "—",
        reference: getReference(msg) || "—",
        messageType: getDisplayType(msg) || msg.messageCode || "—",
        messageFormat: getDisplayFormat(msg) || "—",
        sequenceNumber: msg.sequenceNumber ?? "—",
        creationDate: msg.creationDate || msg.date || "—",
    }), [getReference]);

    const buildMessageSectionData = useCallback((msg, sectionKey, rawCopies = []) => {
        const raw = msg.rawMessage || {};

        if (sectionKey === "header") {
            const pairs = getHeaderExportPairs(msg);
            return {
                label: "Header",
                columns: [{ key: "field", label: "Field" }, { key: "value", label: "Value" }],
                rows: pairs.map(item => ({ field: item.label, value: stringifyExportValue(item.val) })),
            };
        }

        if (sectionKey === "body") {
            const pairs = [
                ["Message Code", msg.messageCode || getDisplayType(msg)],
                ["Message Type", getDisplayFormat(msg)],
                ["Network Protocol", msg.networkProtocol || msg.network],
                ["Network Channel", msg.networkChannel || msg.backendChannel],
                ["Network Priority", msg.networkPriority],
                ["Country", msg.country],
                ["Owner", msg.owner || msg.ownerUnit],
                ["Workflow", msg.workflow],
                ["Direction", formatDirection(msg.io || msg.direction)],
                ["Status", msg.status],
                ["Phase", msg.phase],
                ["Action", msg.action],
                ["Reason", msg.reason],
                ["Environment", msg.environment],
                ["Session No.", msg.sessionNumber],
                ["Sequence No.", msg.sequenceNumber],
            ];
            return {
                label: "Body",
                columns: [{ key: "field", label: "Field" }, { key: "value", label: "Value" }],
                rows: pairs.map(([field, value]) => ({ field, value: stringifyExportValue(value || "—") })),
            };
        }

        if (sectionKey === "history") {
            const lines = msg.historyLines || raw.historyLines || [];
            return {
                label: "History",
                columns: [
                    { key: "dateTime", label: "Date Time" },
                    { key: "phase", label: "Phase" },
                    { key: "action", label: "Action" },
                    { key: "reason", label: "Reason" },
                    { key: "entity", label: "Entity" },
                    { key: "channel", label: "Channel" },
                    { key: "user", label: "User" },
                    { key: "comment", label: "Comment" },
                ],
                rows: lines.map((line) => ({
                    dateTime: line.historyDate ? new Date(line.historyDate).toLocaleString("en-US", { year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: true }) : "—",
                    phase: line.phase || "—",
                    action: line.action || "—",
                    reason: line.reason || "—",
                    entity: line.entity || "—",
                    channel: line.channel || "—",
                    user: line.user || "—",
                    comment: line.comment || "—",
                })),
            };
        }

        if (sectionKey === "payload") {
            const lines = msg.block4Fields || raw.mtPayload?.block4Fields || [];
            return {
                label: "Extended text",
                columns: [
                    { key: "tag", label: "Tag" },
                    { key: "label", label: "Field Label" },
                    { key: "rawValue", label: "Raw Value" },
                ],
                rows: lines.map((line) => ({
                    tag: line.tag || "—",
                    label: line.label || (line.tag ? `Tag ${line.tag}` : "—"),
                    rawValue: line.rawValue || "—",
                })),
            };
        }

        if (sectionKey === "rawpayload") {
            return {
                label: "Raw Payload",
                columns: [{ key: "field", label: "Field" }, { key: "value", label: "Value" }],
                rows: [{ field: "Raw Payload", value: stringifyExportValue(getRawPayloadText(msg)) }],
            };
        }

        if (sectionKey === "details") {
            const ordered = getDetailPairs(msg);
            return {
                label: "All Fields",
                columns: [{ key: "field", label: "Field" }, { key: "value", label: "Value" }],
                rows: ordered.map(item => ({ field: item.label, value: stringifyExportValue(item.val) })),
            };
        }

        if (sectionKey === "rawcopies") {
            return {
                label: "Raw Copies",
                columns: [
                    { key: "messageReference", label: "Message Reference" },
                    { key: "messageTypeCode", label: "Type" },
                    { key: "direction", label: "Direction" },
                    { key: "currentStatus", label: "Status" },
                    { key: "senderAddress", label: "Sender" },
                    { key: "receiverAddress", label: "Receiver" },
                    { key: "protocol", label: "Protocol" },
                    { key: "receivedAt", label: "Received At" },
                    { key: "inputType", label: "Input Type" },
                    { key: "source", label: "Source" },
                    { key: "rawInput", label: "Raw Input" },
                ],
                rows: (rawCopies || []).map((row) => ({
                    messageReference: row.messageReference || "—",
                    messageTypeCode: row.messageTypeCode || "—",
                    direction: row.direction || "—",
                    currentStatus: row.currentStatus || "—",
                    senderAddress: row.senderAddress || "—",
                    receiverAddress: row.receiverAddress || "—",
                    protocol: row.protocol || "—",
                    receivedAt: fmtDate(row.receivedAt || row.ampDateReceived),
                    inputType: row.inputType || "—",
                    source: row.source || "—",
                    rawInput: row.rawInput || "—",
                })),
            };
        }

        return { label: "Section", columns: [{ key: "field", label: "Field" }, { key: "value", label: "Value" }], rows: [] };
    }, [getDisplayType, getDisplayFormat]);

    const buildSectionDataset = useCallback(async (messages, sectionKey) => {
        if (sectionKey === "table") return buildResultTableDataset(messages);

        const needsRawCopies = sectionKey === "rawcopies" || sectionKey === "allcomponents";
        const rawCopyMap = needsRawCopies
            ? await fetchRawCopiesByRefs(messages.map(msg => msg.reference || msg.messageReference).filter(Boolean))
            : new Map();

        const baseColumns = [
            { key: "messageReference", label: "Message Reference" },
            { key: "reference", label: "Reference" },
            { key: "messageType", label: "Message Type" },
            { key: "messageFormat", label: "Message Format" },
            { key: "sequenceNumber", label: "Seq No." },
            { key: "creationDate", label: "Creation Date" },
        ];
        const sectionBaseColumns = sectionKey === "rawcopies"
            ? baseColumns.filter(col => col.key !== "reference")
            : baseColumns;

        if (sectionKey === "allcomponents") {
            const blockKeys = ["header", "history", "payload", "rawpayload", "details", "rawcopies"];
            const keySet = new Set(["messageReference", "reference", "messageType", "messageFormat", "sequenceNumber", "creationDate", "section"]);
            const rows = [];

            messages.forEach(msg => {
                const meta = getExportMessageMeta(msg);
                const rcRows = rawCopyMap.get(msg.reference || msg.messageReference || "") || [];
                blockKeys.forEach(blockKey => {
                    const block = buildMessageSectionData(msg, blockKey, rcRows);
                    block.rows.forEach((row) => {
                        const merged = { ...meta, section: block.label };
                        Object.entries(row).forEach(([key, value]) => {
                            merged[key] = value;
                            keySet.add(key);
                        });
                        rows.push(merged);
                    });
                });
            });

            const columns = [...keySet].map(key => ({ key, label: toPrettyLabel(key) }));
            return { label: "All Components", columns, rows };
        }

        const sampleBlock = buildMessageSectionData(messages[0] || {}, sectionKey, []);
        const columns = [
            ...sectionBaseColumns,
            ...sampleBlock.columns,
        ];
        const rows = [];

        messages.forEach(msg => {
            const meta = getExportMessageMeta(msg);
            const rcRows = rawCopyMap.get(msg.reference || msg.messageReference || "") || [];
            const block = buildMessageSectionData(msg, sectionKey, rcRows);
            block.rows.forEach((row) => {
                rows.push({ ...meta, ...row });
            });
        });

        return { label: sampleBlock.label, columns, rows };
    }, [buildMessageSectionData, buildResultTableDataset, fetchRawCopiesByRefs, getExportMessageMeta]);

    const getExportTargetLabel = useCallback((targetKey) => (
        MAIN_EXPORT_TARGETS.find(target => target.key === targetKey)?.label || toPrettyLabel(targetKey)
    ), []);

    const buildOrderedSectionDataset = useCallback(async (messages, targetKeys) => {
        const orderedKeys = Array.isArray(targetKeys) && targetKeys.length ? targetKeys : ["table"];
        if (orderedKeys.length === 1) return buildSectionDataset(messages, orderedKeys[0]);

        const needsRawCopies = orderedKeys.includes("rawcopies");
        const rawCopyMap = needsRawCopies
            ? await fetchRawCopiesByRefs(messages.map(msg => msg.reference || msg.messageReference).filter(Boolean))
            : new Map();

        const preferredKeys = ["messageReference", "reference", "messageType", "messageFormat", "sequenceNumber", "creationDate", "section"];
        const keySet = new Set(preferredKeys);
        const rows = [];

        messages.forEach((msg) => {
            const meta = getExportMessageMeta(msg);
            const rcRows = rawCopyMap.get(msg.reference || msg.messageReference || "") || [];

            orderedKeys.forEach((targetKey) => {
                const block = targetKey === "table"
                    ? buildResultTableDataset([msg])
                    : buildMessageSectionData(msg, targetKey, rcRows);

                block.rows.forEach((row) => {
                    const merged = { ...meta, section: block.label };
                    Object.entries(row).forEach(([key, value]) => {
                        merged[key] = value;
                        keySet.add(key);
                    });
                    rows.push(merged);
                });
            });
        });

        const orderedColumns = [
            ...preferredKeys.filter(key => keySet.has(key)),
            ...[...keySet].filter(key => !preferredKeys.includes(key)),
        ];
        const columns = orderedColumns.map(key => ({ key, label: toPrettyLabel(key) }));
        const label = orderedKeys.map(getExportTargetLabel).join(" + ");
        return { label, columns, rows };
    }, [buildMessageSectionData, buildResultTableDataset, fetchRawCopiesByRefs, getExportMessageMeta, getExportTargetLabel]);

    const buildOrderedSectionJson = useCallback(async (messages, targetKeys) => {
        const orderedKeys = Array.isArray(targetKeys) && targetKeys.length ? targetKeys : ["table"];
        const needsRawCopies = orderedKeys.includes("rawcopies");
        const rawCopyMap = needsRawCopies
            ? await fetchRawCopiesByRefs(messages.map(msg => msg.reference || msg.messageReference).filter(Boolean))
            : new Map();

        return messages.map((msg) => {
            const meta = getExportMessageMeta(msg);
            const rcRows = rawCopyMap.get(msg.reference || msg.messageReference || "") || [];
            return {
                messageReference: meta.messageReference,
                sections: orderedKeys.map((targetKey) => {
                    const block = targetKey === "table"
                        ? buildResultTableDataset([msg])
                        : buildMessageSectionData(msg, targetKey, rcRows);
                    return {
                        section: getExportTargetLabel(targetKey),
                        data: buildSectionJsonData({ targetKey, block, meta }),
                    };
                }),
            };
        });
    }, [buildMessageSectionData, buildResultTableDataset, fetchRawCopiesByRefs, getExportMessageMeta, getExportTargetLabel]);

    const exportOrderedSectionsWord = useCallback(async ({ messages, orderedKeys, fileBaseName, exportTitle }) => {
        const needsRawCopies = orderedKeys.includes("rawcopies");
        const rawCopyMap = needsRawCopies
            ? await fetchRawCopiesByRefs(messages.map(msg => msg.reference || msg.messageReference).filter(Boolean))
            : new Map();

        const documents = messages.map((msg, msgIdx) => {
            const meta = getExportMessageMeta(msg);
            const rcRows = rawCopyMap.get(msg.reference || msg.messageReference || "") || [];
            const sections = orderedKeys.map((targetKey) => {
                const block = targetKey === "table"
                    ? buildResultTableDataset([msg])
                    : buildMessageSectionData(msg, targetKey, rcRows);
                const blockColumns = targetKey === "rawcopies"
                    ? block.columns.filter((column) => !["senderAddress", "receiverAddress", "protocol", "receivedAt", "inputType", "source"].includes(column.key))
                    : block.columns;
                const isPairBlock = blockColumns.length === 2 && blockColumns.some(col => col.key === "field") && blockColumns.some(col => col.key === "value");

                if (targetKey === "rawpayload") {
                    return { label: block.label, type: "raw", rawText: stringifyExportValue(getRawPayloadText(msg) || "—") || "—" };
                }

                if (isPairBlock) {
                    return {
                        label: block.label,
                        type: "pair",
                        columns: blockColumns,
                        rows: block.rows.map((row) => ({
                            field: stringifyExportValue(row.field || "—") || "—",
                            value: stringifyExportValue(row.value || "—") || "—",
                        })),
                    };
                }

                if (block.rows.length === 1 && blockColumns.length > 4) {
                    const single = block.rows[0] || {};
                    return {
                        label: block.label,
                        type: "pair",
                        columns: [{ key: "field", label: "Field" }, { key: "value", label: "Value" }],
                        rows: blockColumns.map((column) => ({
                            field: column.label,
                            value: stringifyExportValue(single[column.key] ?? "—") || "—",
                        })),
                    };
                }

                const safeColumns = getWordRenderableColumns(blockColumns, block.rows);
                return {
                    label: block.label,
                    type: "table",
                    columns: safeColumns,
                    rows: (block.rows.length ? block.rows : [{}]).map((row) => Object.fromEntries(
                        safeColumns.map((column) => [column.key, stringifyExportValue(row?.[column.key] ?? "—") || "—"])
                    )),
                };
            });

            return {
                pageBreak: msgIdx > 0,
                pageLabel: `Page ${msgIdx + 1}`,
                messageRef: meta.messageReference || getReference(msg) || "—",
                summaryLine: [meta.messageType, meta.messageFormat, formatDirection(msg.io || msg.direction)].filter(Boolean).join("   "),
                sections,
            };
        });

        const html = buildWordComponentExportHtml({
            title: exportTitle,
            documents,
        });
        triggerWordDownload(html, fileBaseName);
    }, [buildMessageSectionData, buildResultTableDataset, fetchRawCopiesByRefs, formatDirection, getExportMessageMeta, getReference]);

    const exportOrderedSectionsPdf = useCallback(async ({ messages, orderedKeys, fileBaseName, exportTitle }) => {
        await loadScriptOnce("https://cdnjs.cloudflare.com/ajax/libs/jspdf/2.5.1/jspdf.umd.min.js", () => !!window.jspdf?.jsPDF);
        await loadScriptOnce("https://cdnjs.cloudflare.com/ajax/libs/jspdf-autotable/3.8.2/jspdf.plugin.autotable.min.js", () => !!window.jspdf?.jsPDF?.API?.autoTable);

        const needsRawCopies = orderedKeys.includes("rawcopies");
        const rawCopyMap = needsRawCopies
            ? await fetchRawCopiesByRefs(messages.map(msg => msg.reference || msg.messageReference).filter(Boolean))
            : new Map();

        const { jsPDF } = window.jspdf;
        const doc = new jsPDF({ orientation: "portrait", unit: "pt", format: "a4" });
        const margin = { top: 48, right: 34, bottom: 34, left: 34 };
        const pageWidth = doc.internal.pageSize.getWidth();
        const pageHeight = doc.internal.pageSize.getHeight();
        const contentWidth = pageWidth - margin.left - margin.right;
        let cursorY = margin.top;

        const drawPageHeader = () => {
            doc.setFont("helvetica", "bold");
            doc.setFont("helvetica", "normal");
            doc.setFontSize(8);
            doc.text(`Page ${doc.getNumberOfPages()}`, pageWidth - margin.right, 28, { align: "right" });
        };

        const resetPageCursor = () => {
            cursorY = margin.top;
            drawPageHeader();
        };

        const ensureSpace = (neededHeight) => {
            if (cursorY + neededHeight <= pageHeight - margin.bottom) return;
            doc.addPage();
            resetPageCursor();
        };

        const drawSectionHeading = (label) => {
            ensureSpace(20);
            doc.setFont("helvetica", "bold");
            doc.setFontSize(10);
            doc.setTextColor(33, 56, 99);
            doc.text(label, margin.left, cursorY);
            cursorY += 8;
        };

        const drawPairTable = (pairs) => {
            doc.autoTable({
                startY: cursorY,
                margin,
                tableWidth: contentWidth,
                theme: "grid",
                head: [["Field", "Value"]],
                body: pairs.length ? pairs : [["Field", "—"]],
                styles: {
                    fontSize: 8.25,
                    cellPadding: { top: 4, right: 5, bottom: 4, left: 5 },
                    overflow: "linebreak",
                    valign: "top",
                    lineColor: [226, 232, 240],
                    lineWidth: 0.35,
                },
                headStyles: {
                    fillColor: [243, 244, 246],
                    textColor: [55, 65, 81],
                    fontStyle: "bold",
                    fontSize: 8.25,
                },
                columnStyles: {
                    0: { cellWidth: 170, fontStyle: "bold", textColor: [100, 116, 139] },
                    1: { cellWidth: contentWidth - 170, textColor: [17, 24, 39] },
                },
                didDrawPage: () => drawPageHeader(),
            });
            cursorY = (doc.lastAutoTable?.finalY || cursorY) + 12;
        };

        const pruneEmptyColumns = (columns, rows) => {
            const alwaysKeep = new Set(["index"]);
            return columns.filter((column) => {
                if (alwaysKeep.has(column.key)) return true;
                return rows.some((row) => {
                    const value = stringifyExportValue(row?.[column.key]);
                    return value && value !== "—";
                });
            });
        };

        const drawDataTable = (columns, rows) => {
            const safeColumns = pruneEmptyColumns(columns, rows);
            const widthSeed = estimatePdfColumnWidths(safeColumns, rows);
            const totalWidth = widthSeed.reduce((sum, width) => sum + width, 0) || 1;
            const scale = totalWidth > contentWidth ? contentWidth / totalWidth : 1;
            const scaledWidths = widthSeed.map((width) => Math.max(48, width * scale));
            const fontSize = safeColumns.length > 7 ? 6.5 : safeColumns.length > 4 ? 7.25 : 8;

            doc.autoTable({
                startY: cursorY,
                margin,
                tableWidth: contentWidth,
                theme: "grid",
                head: [safeColumns.map((column) => column.label)],
                body: (rows.length ? rows : [{}]).map((row) => safeColumns.map((column) => stringifyExportValue(row?.[column.key] ?? "—") || "—")),
                styles: {
                    fontSize,
                    cellPadding: { top: 4, right: 4, bottom: 4, left: 4 },
                    overflow: "linebreak",
                    valign: "top",
                    lineColor: [226, 232, 240],
                    lineWidth: 0.35,
                },
                headStyles: {
                    fillColor: [243, 244, 246],
                    textColor: [55, 65, 81],
                    fontStyle: "bold",
                    fontSize,
                },
                columnStyles: Object.fromEntries(scaledWidths.map((width, index) => [index, { cellWidth: width }])),
                didDrawPage: () => drawPageHeader(),
            });
            cursorY = (doc.lastAutoTable?.finalY || cursorY) + 12;
        };

        const drawRawPayloadBlock = (text) => {
            const rawText = stringifyExportValue(text || "—") || "—";
            const lineHeight = 8.5;
            const codePadding = 8;
            const wrappedLines = doc.splitTextToSize(rawText, contentWidth - (codePadding * 2));
            let offset = 0;

            while (offset < wrappedLines.length) {
                ensureSpace(34);
                const availableHeight = pageHeight - margin.bottom - cursorY - (codePadding * 2);
                const linesPerPage = Math.max(1, Math.floor(availableHeight / lineHeight));
                const lineChunk = wrappedLines.slice(offset, offset + linesPerPage);
                const boxHeight = (lineChunk.length * lineHeight) + (codePadding * 2);

                doc.setFillColor(248, 250, 252);
                doc.setDrawColor(226, 232, 240);
                doc.roundedRect(margin.left, cursorY, contentWidth, boxHeight, 6, 6, "FD");
                doc.setFont("courier", "normal");
                doc.setFontSize(7);
                doc.setTextColor(17, 24, 39);
                doc.text(lineChunk, margin.left + codePadding, cursorY + codePadding + 5);

                cursorY += boxHeight + 10;
                offset += linesPerPage;
            }
        };

        resetPageCursor();

        messages.forEach((msg, msgIdx) => {
            if (msgIdx > 0) {
                doc.addPage();
                resetPageCursor();
            }

            const meta = getExportMessageMeta(msg);
            const messageRef = meta.messageReference || getReference(msg) || "—";
            const summaryLine = [meta.messageType, meta.messageFormat, formatDirection(msg.io || msg.direction)].filter(Boolean).join("   ");

            ensureSpace(30);
            doc.setFont("helvetica", "bold");
            doc.setFontSize(11);
            doc.setTextColor(17, 24, 39);
            doc.text(`Message Ref: ${messageRef}`, margin.left, cursorY);
            cursorY += 14;

            if (summaryLine) {
                doc.setFont("helvetica", "normal");
                doc.setFontSize(8);
                doc.setTextColor(100, 116, 139);
                doc.text(summaryLine, margin.left, cursorY);
                cursorY += 10;
            }

            doc.setDrawColor(203, 213, 225);
            doc.line(margin.left, cursorY, pageWidth - margin.right, cursorY);
            cursorY += 14;

            const rcRows = rawCopyMap.get(msg.reference || msg.messageReference || "") || [];

            orderedKeys.forEach((targetKey) => {
                const block = targetKey === "table"
                    ? buildResultTableDataset([msg])
                    : buildMessageSectionData(msg, targetKey, rcRows);
                const blockColumns = targetKey === "rawcopies"
                    ? block.columns.filter((column) => !["senderAddress", "receiverAddress", "protocol", "receivedAt", "inputType", "source"].includes(column.key))
                    : block.columns;

                drawSectionHeading(getExportTargetLabel(targetKey));

                const isPairBlock = blockColumns.length === 2 && blockColumns.some(col => col.key === "field") && blockColumns.some(col => col.key === "value");
                if (targetKey === "rawpayload") {
                    drawRawPayloadBlock(getRawPayloadText(msg));
                    return;
                }

                if (isPairBlock) {
                    drawPairTable(block.rows.map((row) => [stringifyExportValue(row.field || "—"), stringifyExportValue(row.value || "—")]));
                    return;
                }

                if (block.rows.length === 1 && blockColumns.length > 4) {
                    const single = block.rows[0] || {};
                    drawPairTable(blockColumns.map((column) => [column.label, stringifyExportValue(single[column.key] ?? "—") || "—"]));
                    return;
                }

                drawDataTable(blockColumns, block.rows);
            });
        });

        doc.save(`${fileBaseName}.pdf`);
    }, [buildMessageSectionData, buildResultTableDataset, fetchRawCopiesByRefs, getExportMessageMeta, getExportTargetLabel, getReference]);

    const runExport = async (scope, targetKeys, format) => {
        setShowExportMenu(false);
        setIsExporting(true);
        try {
            const requestedKeys = Array.isArray(targetKeys) && targetKeys.length ? targetKeys : ["table"];
            const orderedKeys = requestedKeys.includes("table") ? ["table"] : requestedKeys;
            if (format === "word" && isWordExportDisabledForTargets(orderedKeys)) {
                throw new Error("Word export is unavailable when Raw Copies is selected.");
            }

            try {
                const job = await createBackendExportJob({ format, scope, targetKeys: orderedKeys });
                window.dispatchEvent(new CustomEvent(EXPORT_JOB_REFRESH_EVENT, { detail: job }));
                showToast(`Export started for ${job.totalCount?.toLocaleString?.() || 0} rows. Track progress in Notifications.`, "info");
                return;
            } catch (jobError) {
                if (!String(jobError?.message || "").includes("Background export is available only above")) {
                    throw jobError;
                }
            }

            const useBackendTableAllExport = scope === "all"
                && orderedKeys.length === 1
                && orderedKeys[0] === "table"
                && SERVER_STREAMED_TABLE_EXPORT_FORMATS.has(format);

            if (useBackendTableAllExport) {
                showToast(`Preparing ${format.toUpperCase()} export on server for ${serverTotal.toLocaleString()} rows…`, "info");
                await downloadBackendTableAllExport(format);
                showToast(`Exported Result Table (${serverTotal.toLocaleString()} rows) as ${format.toUpperCase()}`);
                return;
            }

            let messages;
            if (scope === "all") {
                const label = orderedKeys.map(getExportTargetLabel).join(" + ") || "data";
                showToast(`Fetching all ${serverTotal.toLocaleString()} records for ${label} export…`, "info");
                messages = await fetchAllRows();
            } else {
                messages = getExportRows(scope) || [];
                messages = await ensureDetailedMessages(messages, orderedKeys);
            }

            if (MT_RAW_ONLY_EXPORT_FORMATS.has(format)) {
                if (orderedKeys.length !== 1 || orderedKeys[0] !== "rawpayload") throw new Error("RJE / DOSPCC export is available only when Raw Payload is the only selected section.");
                const result = exportMtRawPayloads({
                    format,
                    messages,
                    fileBaseName: `swift_messages_raw_payload_${scope}`,
                });
                const fmtLabel = format.toUpperCase();
                const skippedBits = [];
                if (result.skippedNonMt) skippedBits.push(`${result.skippedNonMt.toLocaleString()} non-MT skipped`);
                if (result.skippedMissing) skippedBits.push(`${result.skippedMissing.toLocaleString()} missing payload`);
                showToast(`Exported ${result.exportedCount.toLocaleString()} MT raw payload message${result.exportedCount === 1 ? "" : "s"} as ${fmtLabel}${skippedBits.length ? ` (${skippedBits.join(", ")})` : ""}`);
                return;
            }

            if (format === "pdf" && !(orderedKeys.length === 1 && orderedKeys[0] === "table")) {
                const label = orderedKeys.map(getExportTargetLabel).join(" + ");
                const fileBaseName = `swift_messages_${safeFileNamePart(label.toLowerCase().replace(/\s+/g, "_"))}_${scope}`;
                await exportOrderedSectionsPdf({
                    messages,
                    orderedKeys,
                    fileBaseName,
                    exportTitle: `${label} - SWIFT Messages`,
                });
                showToast(`Exported ${label} (${messages.length.toLocaleString()} message${messages.length === 1 ? "" : "s"}) as PDF`);
                return;
            }

            if (format === "word" && !(orderedKeys.length === 1 && orderedKeys[0] === "table")) {
                const label = orderedKeys.map(getExportTargetLabel).join(" + ");
                const fileBaseName = `swift_messages_${safeFileNamePart(label.toLowerCase().replace(/\s+/g, "_"))}_${scope}`;
                await exportOrderedSectionsWord({
                    messages,
                    orderedKeys,
                    fileBaseName,
                    exportTitle: `${label} - SWIFT Messages`,
                });
                showToast(`Exported ${label} (${messages.length.toLocaleString()} message${messages.length === 1 ? "" : "s"}) as Word`);
                return;
            }

            if (format === "json" && !(orderedKeys.length === 1 && orderedKeys[0] === "table")) {
                const label = orderedKeys.map(getExportTargetLabel).join(" + ");
                const payload = await buildOrderedSectionJson(messages, orderedKeys);
                triggerDownload(
                    new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" }),
                    `swift_messages_${safeFileNamePart(label.toLowerCase().replace(/\s+/g, "_"))}_${scope}.json`
                );
                showToast(`Exported ${label} (${messages.length.toLocaleString()} message${messages.length === 1 ? "" : "s"}) as JSON`);
                return;
            }

            const dataset = await buildOrderedSectionDataset(messages, orderedKeys);
            await exportRowsAsFile({
                format,
                rows: dataset.rows,
                columns: dataset.columns,
                fileBaseName: `swift_messages_${safeFileNamePart(dataset.label.toLowerCase().replace(/\s+/g, "_"))}_${scope}`,
                title: `${dataset.label} - SWIFT Messages`,
                sheetName: dataset.label,
                forceGenericPdf: orderedKeys.length > 1,
            });
            showToast(`Exported ${dataset.label} (${dataset.rows.length.toLocaleString()} rows) as ${format.toUpperCase()}`);
        } catch (e) {
            showToast(`Export failed: ${e.message}`, "error");
        } finally {
            setIsExporting(false);
        }
    };

    const summaryStats = !showResult ? []
        : searchMode === "fixed" ? [
            { label: "Total Messages", value: serverTotal, color: "var(--black)" },
        ]
        : searchMode === "advanced" ? [
            {label:"Total",   value:serverTotal,color:"var(--black)"},
            {label:"Accepted",value:processed.filter(m=>m.status==="ACCEPTED"||m.status==="DELIVERED").length,color:"var(--ok)"},
            {label:"Pending", value:processed.filter(m=>["PENDING","PROCESSING","REPAIR"].includes(m.status)).length,color:"var(--warn)"},
            {label:"Failed",  value:processed.filter(m=>["REJECTED","FAILED"].includes(m.status)).length,color:"var(--danger)"},
        ]
        : [];

    const renderCell=(col,msg)=>{
        const value=msg[col.key];
        if(col.key==="format")           { const d=getDisplayFormat(msg); return highlightText?highlight(d,highlightText):d; }
        if(col.key==="type")             { const d=getDisplayType(msg);   return highlightText?highlight(d,highlightText):d; }
        if(col.key==="status")           return <span className={statusCls(value)}>{value??"—"}</span>;
        if(col.key==="amount")           { if(value===undefined||value===null)return "—"; return Number(value).toLocaleString("en-US",{minimumFractionDigits:2,maximumFractionDigits:2}); }
        if(col.key==="direction")        return <span className={`dir-badge ${dirClass(value)}`}>{formatDirection(value)}</span>;
        if(col.key==="sequenceNumber" || col.key==="sessionNumber") return <span style={{fontFamily:"var(--mono)",fontWeight:600}}>{value??"—"}</span>;
        if(col.key==="possibleDuplicate"||col.key==="crossBorder") {
            if(value===true)  return <span style={{color:"var(--danger,#e24b4a)",fontWeight:600,fontSize:12}}>YES</span>;
            if(value===false) return <span style={{color:"var(--ok,#22c55e)",fontWeight:500,fontSize:12}}>NO</span>;
            return "—";
        }
        if(col.key==="amlStatus") {
            if(!value) return "—";
            const color = value==="CLEAR"||value==="CLEAN" ? "var(--ok,#22c55e)" : value==="FLAGGED"||value==="HIGH" ? "var(--danger,#e24b4a)" : "var(--warn,#f97316)";
            return <span style={{color,fontWeight:600,fontSize:12}}>{value}</span>;
        }
        if(col.key==="networkStatus") {
            if(!value) return "—";
            const color = value==="DELIVERED" ? "var(--ok,#22c55e)" : value==="FAILED" ? "var(--danger,#e24b4a)" : "var(--warn,#f97316)";
            return <span style={{color,fontWeight:600,fontSize:12}}>{value}</span>;
        }
        if(value===null||value===undefined) return "—";
        return highlightText?highlight(value,highlightText):String(value);
    };

    const sortIcon=(key)=>{ if(sortKey!==key)return <span className="sort-icon sort-idle">⇅</span>; return <span className="sort-icon sort-active">{sortDir===SORT_ASC?"↑":"↓"}</span>; };
    const activeFilterCount=Object.values(searchState).filter(v=>v!=="").length;
    const extraWidth=180+(shownCols.length>7?(shownCols.length-7)*130:0);
    const scopeTabs=[{key:"all",label:"All",count:serverTotal},{key:"page",label:"This Page",count:currentRecords.length},{key:"selected",label:"Selected",count:selectedRows.size}];
    const orderedExportTargets = exportTargets.length ? exportTargets : ["table"];
    const toggleExportTarget = useCallback((targetKey) => {
        setExportTargets((prev) => {
            if (targetKey === "table") return ["table"];

            const withoutTable = prev.filter(key => key !== "table");
            if (withoutTable.includes(targetKey)) {
                return withoutTable.length === 1 ? withoutTable : withoutTable.filter(key => key !== targetKey);
            }
            return [...withoutTable, targetKey];
        });
    }, []);
    const mainExportFormats = useMemo(() => getExportFormatOptions({
        targetKey: orderedExportTargets.length === 1 ? orderedExportTargets[0] : "",
        includeMtOnly: orderedExportTargets.length === 1 && orderedExportTargets[0] === "rawpayload",
        selectedTargets: orderedExportTargets,
    }), [orderedExportTargets]);

    // ── Advanced field renderer ───────────────────────────────────────────────
    const renderAdvancedField = (fieldKey) => {
        const def = activeFieldDefs.find(f=>f.key===fieldKey) || FIELD_DEFINITIONS.find(f=>f.key===fieldKey);
        if (!def) return null;
        const fieldContent = () => {
            switch(def.type) {
                case "select": {
                    const selectOpts = def.options || def._backendOpts || (def.optKey ? opts[def.optKey] || [] : []);
                    const isStatic   = !!(def.options || def._backendOpts);
                    return <DynSelect value={searchState[def.stateKeys[0]] || ""} onChange={set(def.stateKeys[0])} placeholder={def.placeholder} options={selectOpts} loading={isStatic ? false : optsLoading}/>;
                }
                case "select-type":
                    return <DynSelect value={searchState.type} onChange={set("type")} placeholder="All Types" options={typeOptions} loading={optsLoading}/>;
                case "date-range":
                    return (
                        <div className="adv-date-range-wrap">
                            <DateTimePicker label="From" dateValue={searchState.startDate} timeValue={searchState.startTime}
                                onDateChange={v=>{ setField("startDate", v); if(v){ const autoEnd=addOneMonth(v); if(!searchState.endDate||searchState.endDate>autoEnd) setField("endDate",autoEnd); } }}
                                onTimeChange={v=>setField("startTime",v)} onKeyDown={handleKeyDown}/>
                            <span className="adv-date-sep">→</span>
                            <DateTimePicker label="To" dateValue={searchState.endDate} timeValue={searchState.endTime}
                                onDateChange={v=>{ const clamped=clampToOneMonth(searchState.startDate,v); setField("endDate",clamped); if(clamped!==v) showToast("Max range is 1 month","error"); }}
                                onTimeChange={v=>setField("endTime",v)} onKeyDown={handleKeyDown}/>
                        </div>
                    );
                case "date-range2": {
                    const fromKey=def.stateKeys[0], toKey=def.stateKeys[1];
                    return (
                        <div className="adv-date-range-wrap">
                            <DateTimePicker label="From" dateValue={searchState[fromKey]} timeValue="" onDateChange={v=>setField(fromKey,v)} onTimeChange={()=>{}} onKeyDown={handleKeyDown}/>
                            <span className="adv-date-sep">→</span>
                            <DateTimePicker label="To" dateValue={searchState[toKey]} timeValue="" onDateChange={v=>setField(toKey,v)} onTimeChange={()=>{}} onKeyDown={handleKeyDown}/>
                        </div>
                    );
                }
                case "amount-range":
                    return <div className="adv-range-wrap"><input type="number" placeholder="Min Amount" value={searchState.amountFrom} onChange={set("amountFrom")} onKeyDown={handleKeyDown}/><span className="adv-range-sep">—</span><input type="number" placeholder="Max Amount" value={searchState.amountTo} onChange={set("amountTo")} onKeyDown={handleKeyDown}/></div>;
                case "seq-range":
                    return <div className="adv-range-wrap"><input type="number" placeholder="e.g. 1" value={searchState.seqFrom} onChange={set("seqFrom")} onKeyDown={handleKeyDown}/><span className="adv-range-sep">—</span><input type="number" placeholder="e.g. 9999" value={searchState.seqTo} onChange={set("seqTo")} onKeyDown={handleKeyDown}/></div>;
                case "text-wide":
                    return <input className="input-wide" placeholder={def.placeholder} value={searchState[def.stateKeys[0]]} onChange={set(def.stateKeys[0])} onKeyDown={handleKeyDown}/>;
                default:
                    return <input placeholder={def.placeholder} value={searchState[def.stateKeys[0]]} onChange={set(def.stateKeys[0])} onKeyDown={handleKeyDown}/>;
            }
        };
        return (
            <div key={fieldKey} className="adv-field-card">
                <div className="adv-field-header">
                    <span className="adv-field-label">{def.label}</span>
                    {fieldKey !== "dateRange" && (
                        <button className="adv-field-remove" onClick={()=>removeAdvancedField(fieldKey)} title="Remove field">
                            <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
                        </button>
                    )}
                </div>
                <div className="adv-field-input">{fieldContent()}</div>
            </div>
        );
    };

    const filteredFieldDefs = activeFieldDefs.filter(f=>{
        if (f.key === "dateRange" || advancedFields.includes(f.key)) return false;
        const query = fieldPickerQuery.trim().toLowerCase();
        if (!query) return true;
        const pickerGroup = getAdvancedPickerGroup(f).toLowerCase();
        return f.label.toLowerCase().includes(query)
            || f.group.toLowerCase().includes(query)
            || pickerGroup.includes(query);
    });

    const groupedFields = useMemo(() => {
        return ADV_PICKER_GROUP_ORDER.reduce((acc, groupLabel) => {
            const items = filteredFieldDefs
                .filter(field => getAdvancedPickerGroup(field) === groupLabel)
                .sort((a, b) => a.label.localeCompare(b.label));
            if (items.length) acc[groupLabel] = items;
            return acc;
        }, {});
    }, [filteredFieldDefs]);

    // ── Multi-window modal system ────────────────────────────────────────────
    useEffect(() => {
        window.dispatchEvent(new CustomEvent("swift:modalsOpen", { detail: { open: openModals.length > 0 } }));
    }, [openModals.length]);

    const bringToFront = useCallback((id, z) => {
        if (z) topZRef.current = Math.max(topZRef.current, z);
        else   topZRef.current += 1;
        const finalZ = z || topZRef.current;
        setOpenModals(ms => ms.map(m => m.id === id ? { ...m, zIndex: finalZ } : m));
    }, []);

    // Tracks open msgKeys synchronously — prevents rapid double-clicks slipping through
    // before React commits the state update
    const openedKeysRef = useRef(new Set());

    const openModal = (msg, e, absIdx) => {
        e.stopPropagation();

        // Build a stable, unique key for this message
        const msgKey = String(
            msg.id ||
            msg.transactionReference ||
            msg.reference ||
            msg.mur || msg.userReference ||
            `${msg.sequenceNumber}-${msg.date||""}-${msg.sender||""}-${msg.receiver||""}`
        );

        // Check synchronously — openedKeysRef is updated immediately
        if (openedKeysRef.current.has(msgKey)) {
            // Already open — bring to front + flash
            setOpenModals(ms => {
                const existing = ms.find(m => m.msgKey === msgKey);
                if (!existing) return ms;
                topZRef.current += 1;
                return ms.map(m =>
                    m.id === existing.id
                        ? { ...m, zIndex: topZRef.current, _flash: (m._flash || 0) + 1 }
                        : m
                );
            });
            return;
        }

        // Mark as open immediately (sync) before state update
        openedKeysRef.current.add(msgKey);

        const id = ++modalIdRef.current;
        const count = openedKeysRef.current.size - 1; // -1 because we just added
        const vw = window.innerWidth, vh = window.innerHeight;
        const w = Math.min(880, vw - 80), h = Math.min(680, vh - 80);
        const off = (count % 8) * 30;
        const x = Math.max(20, Math.min(vw - w - 20, (vw - w) / 2 + off));
        const y = Math.max(20, Math.min(vh - h - 20, (vh - h) / 2 + off));
        topZRef.current += 1;
        setOpenModals(ms => [...ms, { id, msg, msgKey, tab: "header", pos: { x, y }, size: { w, h }, zIndex: topZRef.current, index: absIdx }]);

        const ref = msg.reference || msg.messageReference;
        if (ref && !(msg.rawMessage || msg.block4Fields || msg.historyLines)) {
            fetch(`${API_DETAIL_BY_REF_URL}/${encodeURIComponent(ref)}`, { headers: authHeaders() })
                .then(r => { if (!r.ok) throw new Error(`Detail fetch failed (${r.status})`); return r.json(); })
                .then(detail => {
                    if (!detail) return;
                    setOpenModals(ms => ms.map(m => m.id === id ? { ...m, msg: detail } : m));
                })
                .catch(() => {});
        }
    };

    const closeModal = (id) => {
        setOpenModals(ms => {
            const modal = ms.find(m => m.id === id);
            if (modal) openedKeysRef.current.delete(modal.msgKey);
            return ms.filter(m => m.id !== id);
        });
    };
    const closeAllModals = () => {
        openedKeysRef.current.clear();
        setOpenModals([]);
    };
    const patchModal = useCallback((id, patch) => setOpenModals(ms => ms.map(m => m.id === id ? { ...m, ...patch } : m)), []);
    const goModalPrev = useCallback((id) => setOpenModals(ms => ms.map(m => { if(m.id!==id||m.index<=0)return m; return {...m,msg:processed[m.index-1],index:m.index-1,tab:m.tab}; })), [processed]);
    const goModalNext = useCallback((id) => setOpenModals(ms => ms.map(m => { if(m.id!==id||m.index>=processed.length-1)return m; return {...m,msg:processed[m.index+1],index:m.index+1,tab:m.tab}; })), [processed]);

    // ── Render ───────────────────────────────────────────────────────────────
    return (
        <div className="container">
            {toastMsg&&<div className={`toast toast-${toastMsg.type}`}><span>{toastMsg.msg}</span></div>}
            {isFetching&&<div style={{padding:"10px 16px",background:"var(--accent-light)",borderRadius:6,marginBottom:8,fontSize:13,color:"var(--accent)",display:"flex",alignItems:"center",gap:8}}><span className="spinner" style={{borderTopColor:"var(--accent)"}}/>Loading messages from backend...</div>}
            {fetchError &&<div style={{padding:"10px 16px",background:"var(--danger-light)",borderRadius:6,marginBottom:8,fontSize:13,color:"var(--danger)",border:"1px solid var(--danger-border)"}}>&#x26A0; Backend error: {fetchError}. Check that the backend is reachable at {process.env.REACT_APP_API_BASE_URL}</div>}

            {/* ── Header bar ── */}
            <div className="app-header">
                <div className="app-header-actions">
                    <div className="search-mode-toggle">
                        <button className={`mode-btn${searchMode==="fixed"?" mode-btn-active":""}`} onClick={()=>handleModeSwitch("fixed")}>
                            <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round"><line x1="8" y1="6" x2="21" y2="6"/><line x1="8" y1="12" x2="21" y2="12"/><line x1="8" y1="18" x2="21" y2="18"/><line x1="3" y1="6" x2="3.01" y2="6"/><line x1="3" y1="12" x2="3.01" y2="12"/><line x1="3" y1="18" x2="3.01" y2="18"/></svg>
                            Fixed
                        </button>
                        <button className={`mode-btn${searchMode==="advanced"?" mode-btn-active":""}`} onClick={()=>handleModeSwitch("advanced")}>
                            <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/></svg>
                            Advanced
                        </button>
                        <button className={`mode-btn${searchMode==="rawcopies"?" mode-btn-active":""}`} onClick={()=>handleModeSwitch("rawcopies")}>
                            <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="9" y1="13" x2="15" y2="13"/><line x1="9" y1="17" x2="13" y2="17"/></svg>
                            Raw Copies
                        </button>
                    </div>
                    {searchMode !== "rawcopies" && savedSearches.length > 0 && (
                        <button className="hdr-btn" onClick={()=>setShowSavedPanel(!showSavedPanel)}>
                            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M19 21l-7-5-7 5V5a2 2 0 012-2h10a2 2 0 012 2z"/></svg>
                            Saved ({savedSearches.length})
                        </button>
                    )}
                    {searchMode !== "rawcopies" && (
                        <button className="hdr-btn" onClick={saveSearch}>
                            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M19 21H5a2 2 0 01-2-2V5a2 2 0 012-2h11l5 5v11a2 2 0 01-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/></svg>
                            Save Search
                        </button>
                    )}
                </div>
            </div>

            {showSavedPanel&&<div className="saved-panel"><div className="saved-panel-header"><span>Saved Searches</span><button className="icon-btn" onClick={()=>setShowSavedPanel(false)}>&#x2715;</button></div>{savedSearches.map((s,i)=>(<div key={i} className="saved-item"><span className="saved-name">{s.name}</span><span className="saved-ts">{new Date(s.ts).toLocaleDateString()}</span><button className="pg-btn" onClick={()=>loadSearch(s)}>Load</button><button className="icon-btn danger-btn" onClick={()=>deleteSearch(i)}>&#x2715;</button></div>))}</div>}

            {/* ══════════ RAW COPIES MODE ══════════ */}
            {searchMode === "rawcopies" && (<>
                <div className="rc-panel">
                    <div className="rc-panel-title" onClick={() => setRcPanelCollapsed(p => !p)}>
                        <div className="rc-panel-title-left">
                            <span>Raw Copies Search</span>
                            <span className="rc-mode-chip">
                                <svg width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>
                                amp_raw_copies
                            </span>
                            {rcTotal > 0 && <span className="filter-badge">{rcTotal.toLocaleString()} found</span>}
                        </div>
                        <span className="collapse-icon">{rcPanelCollapsed ? "▼ Expand" : "▲ Collapse"}</span>
                    </div>
                    {!rcPanelCollapsed && (
                        <div className="rc-grid">
                            <div className="rc-field"><label>Message Reference</label><input placeholder="e.g. KCRJ48066072DGTF" value={rcFilters.messageReference} onChange={setRcField("messageReference")} onKeyDown={handleRcKey}/></div>
                            <div className="rc-field"><label>Sender BIC</label><input placeholder="e.g. ABNANL2AXXX" value={rcFilters.sender} onChange={setRcField("sender")} onKeyDown={handleRcKey}/></div>
                            <div className="rc-field"><label>Receiver BIC</label><input placeholder="e.g. RBOSGB2LXXX" value={rcFilters.receiver} onChange={setRcField("receiver")} onKeyDown={handleRcKey}/></div>
                            <div className="rc-field rc-field-wide"><label>Free Search (across reference, sender, receiver, raw content)</label>
                                <input placeholder="Search across all fields…" value={rcFilters.freeText} onChange={setRcField("freeText")} onKeyDown={handleRcKey}/>
                            </div>
                        </div>
                    )}
                </div>

                <div className="action-bar">
                    <div className="action-left">
                        <button className={`search-btn${rcLoading ? " btn-loading" : ""}`} onClick={() => doRcSearch(0)} disabled={rcLoading}>
                            {rcLoading ? <><span className="spinner"/>Searching…</> : <><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round"><circle cx="11" cy="11" r="7"/><line x1="16.5" y1="16.5" x2="22" y2="22"/></svg>Search Raw Copies</>}
                        </button>
                        <button className="clear-btn" onClick={handleRcReset}>
                            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round"><polyline points="1 4 1 10 7 10"/><path d="M3.51 15a9 9 0 102.13-9.36L1 10"/></svg>Reset
                        </button>
                    </div>
                    <div className="action-hint">
                        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>
                        Click a row to expand raw XML &#xB7; Grouped by Message Reference
                    </div>
                </div>

                {rcError && <div style={{padding:"10px 16px",background:"var(--danger-light)",borderRadius:6,marginBottom:8,fontSize:13,color:"var(--danger)",border:"1px solid var(--danger-border)"}}>&#x26A0; {rcError}</div>}

                {rcSearched && (<>
                    {rcResults.length > 0 && (
                        <div className="rc-stats-bar">
                            <div className="rc-stat">
                                <span className="rc-stat-value">{rcTotal.toLocaleString()}</span>
                                <span className="rc-stat-label">Total Raw Copies</span>
                            </div>
                        </div>
                    )}

                    {rcResults.length === 0 ? (
                        <div className="rc-empty">
                            <svg width="44" height="44" viewBox="0 0 24 24" fill="none" stroke="var(--gray-4)" strokeWidth="1.5"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
                            <p>No raw copies found</p><span>Try adjusting your search filters</span>
                        </div>
                    ) : (
                        <div className="rc-table-wrap">
                            <table className="rc-table">
                                <thead>
                                    <tr>
                                        <th style={{width:36}}/><th style={{width:40}}>#</th>
                                        <th>Message Reference</th><th>Type</th><th>Direction</th>
                                        <th>Status</th><th>Sender</th><th>Receiver</th>
                                        <th>Protocol</th><th>Received At</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {rcGrouped.map(([ref, rows]) => (
                                        <React.Fragment key={ref}>
                                            <tr className="rc-group-header">
                                                <td colSpan={10}>
                                                    <div className="rc-group-ref">
                                                        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="#1d4ed8" strokeWidth="2.5"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>
                                                        <span className="rc-group-ref-text">{ref}</span>
                                                        <span className="rc-group-badge">
                                                            <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg>
                                                            {rows.length} cop{rows.length === 1 ? "y" : "ies"}
                                                        </span>
                                                    </div>
                                                </td>
                                            </tr>
                                            {rows.map((row, ri) => (
                                                <React.Fragment key={row.id || ri}>
                                                    <tr onClick={() => setRcExpandedRow(p => p === row.id ? null : row.id)}
                                                        style={{background: ri % 2 === 0 ? "var(--white)" : "var(--gray-7)", cursor:"pointer"}}>
                                                        <td>
                                                            <button className={`rc-expand-btn${rcExpandedRow === row.id ? " open" : ""}`}
                                                                onClick={e => { e.stopPropagation(); setRcExpandedRow(p => p === row.id ? null : row.id); }}
                                                                title="View raw content">
                                                                <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><polyline points="9 18 15 12 9 6"/></svg>
                                                            </button>
                                                        </td>
                                                        <td style={{color:"var(--gray-3)",fontWeight:600,fontSize:12}}>{ri + 1}</td>
                                                        <td className="rc-mono" style={{fontSize:11}}>{row.messageReference ? row.messageReference.slice(0,20) + (row.messageReference.length > 20 ? "…" : "") : "—"}</td>
                                                        <td><span style={{fontFamily:"monospace",fontWeight:700,fontSize:12}}>{row.messageTypeCode || "—"}</span></td>
                                                        <td><span className={rcDirCls(row.direction)}>{row.direction || "—"}</span></td>
                                                        <td><span className={rcStatusCls(row.currentStatus)}>{row.currentStatus || "—"}</span></td>
                                                        <td className="rc-mono">{row.senderAddress || "—"}</td>
                                                        <td className="rc-mono">{row.receiverAddress || "—"}</td>
                                                        <td style={{fontSize:12}}>{row.protocol || "—"}</td>
                                                        <td style={{fontSize:12,whiteSpace:"nowrap"}}>{fmtDate(row.receivedAt || row.ampDateReceived)}</td>
                                                    </tr>
                                                    {rcExpandedRow === row.id && (
                                                        <tr className="rc-raw-row">
                                                            <td colSpan={10}>
                                                                <div className="rc-raw-inner">
                                                                    <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:8}}>
                                                                        <div>
                                                                            <div className="rc-raw-label">Raw Input — {row.inputType || "UNKNOWN"} &#xB7; {row.source || "—"}</div>
                                                                            <div style={{fontSize:10,color:"var(--rc-raw-meta)",marginTop:2}}>
                                                                                ID: <span style={{fontFamily:"monospace"}}>{row.id}</span>
                                                                                {row.ampDateReceived && <> &#xB7; AMP received: {fmtDate(row.ampDateReceived)}</>}
                                                                            </div>
                                                                        </div>
                                                                        {row.rawInput && (
                                                                            <button className={`rc-copy-btn${rcCopiedId === row.id ? " copied" : ""}`}
                                                                                onClick={e => { e.stopPropagation(); copyRaw(row.id, row.rawInput); }}>
                                                                                {rcCopiedId === row.id ? "&#x2713; Copied" : "Copy XML"}
                                                                            </button>
                                                                        )}
                                                                    </div>
                                                                    {row.rawInput
                                                                        ? <pre className="rc-raw-xml">{row.rawInput}</pre>
                                                                        : <div style={{color:"var(--rc-raw-meta)",fontStyle:"italic",fontSize:13}}>No raw content available</div>
                                                                    }
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

                    {/* ── PATCHED: Full pagination matching Fixed/Advanced UX ── */}
                    {rcTotalPages >= 1 && (
                        <div className="pagination-bar">
                            <div className="pagination-left">
                                <span className="record-range">
                                    Showing <strong>{rcPage * rcRecordsPerPage + 1}&ndash;{Math.min((rcPage + 1) * rcRecordsPerPage, rcTotal)}</strong> of <strong>{rcTotal.toLocaleString()}</strong> records
                                </span>
                            </div>
                            <div className="pagination-center">
                                <button className="pg-btn pg-edge"
                                    onClick={() => { setRcStartPage(1); doRcSearch(0); }}
                                    disabled={rcPage === 0}>&#xAB;&#xAB;</button>
                                <button className="pg-btn"
                                    onClick={() => { const p = Math.max(0, rcPage - 1); setRcStartPage(Math.floor(p / rcPagesPerGroup) * rcPagesPerGroup + 1); doRcSearch(p); }}
                                    disabled={rcPage === 0}>&#x2039; Prev</button>
                                {rcStartPage > 1 && <span className="pg-ellipsis">…</span>}
                                {[...Array(rcPagesPerGroup)].map((_, i) => {
                                    const p = rcStartPage - 1 + i;
                                    if (p >= rcTotalPages) return null;
                                    return (
                                        <button key={p}
                                            className={`pg-btn pg-num${rcPage === p ? " pg-active" : ""}`}
                                            onClick={() => { setRcStartPage(Math.floor(p / rcPagesPerGroup) * rcPagesPerGroup + 1); doRcSearch(p); }}>
                                            {p + 1}
                                        </button>
                                    );
                                })}
                                {rcStartPage + rcPagesPerGroup - 1 < rcTotalPages && <span className="pg-ellipsis">…</span>}
                                <button className="pg-btn"
                                    onClick={() => { const p = Math.min(rcTotalPages - 1, rcPage + 1); setRcStartPage(Math.floor(p / rcPagesPerGroup) * rcPagesPerGroup + 1); doRcSearch(p); }}
                                    disabled={rcPage >= rcTotalPages - 1}>Next &#x203A;</button>
                                <button className="pg-btn pg-edge"
                                    onClick={() => { const p = rcTotalPages - 1; setRcStartPage(Math.floor(p / rcPagesPerGroup) * rcPagesPerGroup + 1); doRcSearch(p); }}
                                    disabled={rcPage >= rcTotalPages - 1}>&#xBB;&#xBB;</button>
                            </div>
                            <div className="pagination-right">
                                <label className="pg-label">Go to</label>
                                <input className="pg-goto" type="number" min="1" max={rcTotalPages}
                                    value={rcGoToPage} placeholder="pg"
                                    onChange={e => setRcGoToPage(e.target.value)}
                                    onKeyDown={e => {
                                        if (e.key === "Enter") {
                                            const p = parseInt(rcGoToPage) - 1;
                                            if (p >= 0 && p < rcTotalPages) {
                                                setRcStartPage(Math.floor(p / rcPagesPerGroup) * rcPagesPerGroup + 1);
                                                doRcSearch(p);
                                            }
                                            setRcGoToPage("");
                                        }
                                    }}
                                />
                                <span className="pg-of-total">of {rcTotalPages}</span>
                                <span className="pg-divider"/>
                                <label className="pg-label">Rows</label>
                                <select className="pg-rows-select" value={rcRecordsPerPage}
                                    onChange={e => {
                                        const nextSize = Number(e.target.value);
                                        setRcRecordsPerPage(nextSize);
                                        setRcPage(0); setRcStartPage(1);
                                        doRcSearch(0, nextSize);
                                    }}>
                                    <option value={10}>10</option>
                                    <option value={20}>20</option>
                                    <option value={50}>50</option>
                                    <option value={100}>100</option>
                                </select>
                            </div>
                        </div>
                    )}
                </>)}
            </>)}

            {/* ══════════ FIXED SEARCH PANEL ══════════ */}
            {searchMode==="fixed"&&(
                <div className={`search-panel${panelCollapsed?" panel-collapsed":""}`}>
                    <div className="panel-section-title" onClick={()=>setPanelCollapsed(p=>!p)} style={{cursor:"pointer"}}>
                        <span>Search Criteria {activeFilterCount>0&&<span className="filter-badge">{activeFilterCount} active</span>}</span>
                        <span className="collapse-icon">{panelCollapsed?"▼ Expand":"▲ Collapse"}</span>
                    </div>
                    {!panelCollapsed&&(<>
                        <div className="row">
                            <div className="field-group"><label>Message Format</label><DynSelect value={searchState.format} onChange={e=>setSearchState(s=>({...s,format:e.target.value,type:"",messageCode:""}))} placeholder="All Formats" options={opts.formats} loading={optsLoading}/></div>
                            <div className="field-group"><label>Message Type</label><select value={searchState.type} onChange={set("type")} onKeyDown={handleKeyDown}><option value="">All Types</option>{typeOptions.map(t=><option key={t} value={t}>{t}</option>)}</select></div>
                            <DateTimePicker label="Starting Date / Time" dateValue={searchState.startDate} timeValue={searchState.startTime} onDateChange={v=>setField("startDate",v)} onTimeChange={v=>setField("startTime",v)} onKeyDown={handleKeyDown}/>
                            <DateTimePicker label="Ending Date / Time"   dateValue={searchState.endDate}   timeValue={searchState.endTime}   onDateChange={v=>setField("endDate",v)}   onTimeChange={v=>setField("endTime",v)}   onKeyDown={handleKeyDown}/>
                            <div className="field-group"><label>User Reference (MUR)</label><input placeholder="MUR" value={searchState.userReference} onChange={set("userReference")} onKeyDown={handleKeyDown}/></div>
                        </div>
                        <div className="row">
                            <div className="field-group"><label>Source System</label><DynSelect value={searchState.sourceSystem} onChange={set("sourceSystem")} placeholder="All Systems" options={opts.sourceSystems} loading={optsLoading}/></div>
                            <div className="field-group"><label>RFK Reference / UMID</label><input placeholder="Enter RFK Reference" value={searchState.rfkReference} onChange={set("rfkReference")} onKeyDown={handleKeyDown}/></div>
                            <div className="field-group"><label>Message Direction</label><DynSelect value={searchState.direction} onChange={set("direction")} placeholder="All Directions" options={opts.directions.length?opts.directions:opts.ioDirections} loading={optsLoading}/></div>
                            <div className="field-group"><label>Status</label><DynSelect value={searchState.status} onChange={set("status")} placeholder="All Status" options={opts.statuses} loading={optsLoading}/></div>
                            <div className="field-group"><label>FIN-COPY</label><DynSelect value={searchState.finCopy} onChange={set("finCopy")} placeholder="All" options={opts.finCopies} loading={optsLoading}/></div>
                            <div className="field-group"><label>Network</label><DynSelect value={searchState.network} onChange={set("network")} placeholder="All Networks" options={opts.networks.length?opts.networks:opts.networkProtocols||[]} loading={optsLoading}/></div>
                        </div>
                        <div className="row">
                            <div className="field-group"><label>Sender BIC</label><input placeholder="Enter Sender BIC" value={searchState.sender} onChange={set("sender")} onKeyDown={handleKeyDown}/></div>
                            <div className="field-group"><label>Receiver BIC</label><input placeholder="Enter Receiver BIC" value={searchState.receiver} onChange={set("receiver")} onKeyDown={handleKeyDown}/></div>
                            <div className="field-group"><label>Phase</label><DynSelect value={searchState.phase} onChange={set("phase")} placeholder="All Phases" options={opts.phases} loading={optsLoading}/></div>
                            <div className="field-group"><label>Action</label><DynSelect value={searchState.action} onChange={set("action")} placeholder="All Actions" options={opts.actions} loading={optsLoading}/></div>
                            <div className="field-group"><label>Reason</label><input placeholder="Enter Reason" value={searchState.reason} onChange={set("reason")} onKeyDown={handleKeyDown}/></div>
                            <div className="field-group"><label>Correspondent</label><input placeholder="Correspondent" value={searchState.correspondent} onChange={set("correspondent")} onKeyDown={handleKeyDown}/></div>
                        </div>
                        <div className="row">
                            <div className="field-group"><label>Amount From</label><input type="number" placeholder="Min Amount" value={searchState.amountFrom} onChange={set("amountFrom")} onKeyDown={handleKeyDown}/></div>
                            <div className="field-group"><label>Amount To</label><input type="number" placeholder="Max Amount" value={searchState.amountTo} onChange={set("amountTo")} onKeyDown={handleKeyDown}/></div>
                            <div className="field-group"><label>Currency (CCY)</label><DynSelect value={searchState.currency} onChange={set("currency")} placeholder="All Currencies" options={opts.currencies} loading={optsLoading}/></div>
                            <div className="field-group"><label>Owner / Unit</label><DynSelect value={searchState.ownerUnit} onChange={set("ownerUnit")} placeholder="All Units" options={opts.ownerUnits} loading={optsLoading}/></div>
                            <div className="field-group"><label>Message Reference</label><input placeholder="Message Reference" value={searchState.messageReference} onChange={set("messageReference")} onKeyDown={handleKeyDown}/></div>
                        </div>
                        <div className="row">
                            <div className="field-group"><label>Seq No. From</label><input type="number" placeholder="e.g. 1" value={searchState.seqFrom} onChange={set("seqFrom")} onKeyDown={handleKeyDown}/></div>
                            <div className="field-group"><label>Seq No. To</label><input type="number" placeholder="e.g. 9999" value={searchState.seqTo} onChange={set("seqTo")} onKeyDown={handleKeyDown}/></div>
                            <div className="field-group"><label>Session No.</label><input placeholder="e.g. 0001" value={searchState.sessionNumber} onChange={set("sessionNumber")} onKeyDown={handleKeyDown}/></div>
                            <div className="field-group"><label>Logical Terminal</label><input placeholder="e.g. BPXAINAAXPUN" value={searchState.logicalTerminalAddress} onChange={set("logicalTerminalAddress")} onKeyDown={handleKeyDown}/></div>
                            <div className="field-group field-group-wide"><label>UETR</label><input className="input-wide" placeholder="Enter UETR (e.g. 8a562c65-...)" value={searchState.uetr} onChange={set("uetr")} onKeyDown={handleKeyDown}/></div>
                        </div>
                        <div className="row" style={{alignItems:"flex-end"}}>
                            <div className="field-group field-group-wide"><label>Free Search Text</label><input className="input-wide" placeholder="Searches across all fields..." value={searchState.freeSearchText} onChange={set("freeSearchText")} onKeyDown={handleKeyDown}/></div>
                            <div className="field-group"><label>Channel / Session</label><DynSelect value={searchState.backendChannel} onChange={set("backendChannel")} placeholder="All Channels" options={opts.backendChannels.length?opts.backendChannels:opts.networkChannels} loading={optsLoading}/></div>
                        </div>
                    </>)}
                </div>
            )}

            {/* ══════════ ADVANCED SEARCH PANEL ══════════ */}
            {searchMode==="advanced"&&(
                <div className={`search-panel adv-panel${panelCollapsed?" panel-collapsed":""}`}>
                    <div className="panel-section-title" onClick={()=>setPanelCollapsed(p=>!p)} style={{cursor:"pointer"}}>
                        <div style={{display:"flex",alignItems:"center",gap:10}}>
                            <span>Advanced Search</span>
                            {advancedFields.length>0&&<span className="filter-badge">{advancedFields.length} field{advancedFields.length!==1?"s":""}</span>}
                            <span className="adv-mode-chip"><svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/></svg>Dynamic</span>
                        </div>
                        <span className="collapse-icon">{panelCollapsed?"▼ Expand":"▲ Collapse"}</span>
                    </div>
                    {!panelCollapsed&&(<>
                        <div className="adv-toolbar">
                            <div className="adv-picker-wrap" ref={fieldPickerRef}>
                                <button className="adv-add-btn" onClick={()=>setShowFieldPicker(p=>!p)}>
                                    <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
                                    Add Search Field
                                    <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" style={{marginLeft:2}}><polyline points="6 9 12 15 18 9"/></svg>
                                </button>
                                {showFieldPicker&&(
                                    <div className="adv-picker-dropdown">
                                        <div className="adv-picker-search">
                                            <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round"><circle cx="11" cy="11" r="7"/><line x1="16.5" y1="16.5" x2="22" y2="22"/></svg>
                                            <input placeholder="Search fields..." value={fieldPickerQuery} onChange={e=>setFieldPickerQuery(e.target.value)} autoFocus/>
                                            {fieldPickerQuery&&<button className="adv-picker-clear" onClick={()=>setFieldPickerQuery("")}>&#x2715;</button>}
                                        </div>
                                        <div className="adv-picker-body">
                                            {Object.keys(groupedFields).length===0&&<div className="adv-picker-empty">{advancedFields.length===FIELD_DEFINITIONS.length?"All fields added":"No fields match"}</div>}
                                            {Object.entries(groupedFields).map(([group,items])=>(
                                                <div key={group} className="adv-picker-group">
                                                    <div className="adv-picker-group-label">{group}</div>
                                                    {items.map(f=>(
                                                        <button key={f.key} className="adv-picker-item" onClick={()=>addAdvancedField(f.key)}>
                                                            <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
                                                            {f.label}
                                                        </button>
                                                    ))}
                                                </div>
                                            ))}
                                        </div>
                                    </div>
                                )}
                            </div>
                            {advancedFields.length>0&&(<button className="adv-clear-fields-btn" onClick={()=>{setAdvancedFields(["dateRange"]);setSearchState(initialSearchState);}}><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><polyline points="1 4 1 10 7 10"/><path d="M3.51 15a9 9 0 102.13-9.36L1 10"/></svg>Clear all fields</button>)}
                            <div className="adv-info-text"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>Result table shows only columns for selected fields</div>
                        </div>
                        <div className="adv-fixed-date-wrap">{renderAdvancedField("dateRange")}</div>
                        {advancedFields.filter(f=>f!=="dateRange").length===0&&(
                            <div className="adv-empty-state">
                                <svg width="36" height="36" viewBox="0 0 24 24" fill="none" stroke="var(--gray-4)" strokeWidth="1.5"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/><line x1="8" y1="11" x2="14" y2="11"/><line x1="11" y1="8" x2="11" y2="14"/></svg>
                                <p>No search fields added yet</p><span>Click "Add Search Field" to choose which fields to search on</span>
                            </div>
                        )}
                        {advancedFields.filter(f=>f!=="dateRange").length>0&&(
                            <div className="adv-fields-grid">{advancedFields.filter(f=>f!=="dateRange").map(fkey=>renderAdvancedField(fkey))}</div>
                        )}
                    </>)}
                </div>
            )}

            {/* ── Action Bar (Fixed / Advanced only) ── */}
            {searchMode !== "rawcopies" && (
                <div className="action-bar">
                    <div className="action-left">
                        <button className={`search-btn${isSearching?" btn-loading":""}`} onClick={()=>handleSearch()} disabled={isSearching}>
                            {isSearching?(<><span className="spinner"/>Searching...</>):(<><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round"><circle cx="11" cy="11" r="7"/><line x1="16.5" y1="16.5" x2="22" y2="22"/></svg>Search</>)}
                        </button>
                        <button className="clear-btn" onClick={handleClear}><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round"><polyline points="1 4 1 10 7 10"/><path d="M3.51 15a9 9 0 102.13-9.36L1 10"/></svg>Reset</button>
                    </div>
                    <div style={{display:"flex",alignItems:"center",gap:16}}>
                        {searchMode==="advanced"&&advancedFields.length>0&&(
                            <div className="adv-active-fields-strip">{advancedFields.map(fkey=>{ const def=FIELD_DEFINITIONS.find(f=>f.key===fkey); return def?<span key={fkey} className="adv-active-chip">{def.label}</span>:null; })}</div>
                        )}
                        <div className="action-hint"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>Press Enter in any field to search</div>
                    </div>
                </div>
            )}

            {/* ── Results (Fixed / Advanced only) ── */}
            {searchMode !== "rawcopies" && showResult&&(<>
                <div className="stats-row">
                    {summaryStats.map((s,i)=>(<div key={i} className="stat-card" style={{"--stat-color":s.color}}><span className="stat-value">{s.value.toLocaleString()}</span><span className="stat-label">{s.label}</span></div>))}
                    <div className="stats-spacer"/>
                    {searchMode==="fixed"&&(
                        <div className="col-manager-wrap" ref={colManagerRef}>
                            <button className="tool-btn" onClick={()=>setShowColManager(p=>!p)}><svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><line x1="8" y1="6" x2="21" y2="6"/><line x1="8" y1="12" x2="21" y2="12"/><line x1="8" y1="18" x2="21" y2="18"/><line x1="3" y1="6" x2="3.01" y2="6"/><line x1="3" y1="12" x2="3.01" y2="12"/><line x1="3" y1="18" x2="3.01" y2="18"/></svg>Columns ({shownCols.length}/{allColumns.length})</button>
                            {showColManager&&<div className="col-manager-dropdown">
                                <div className="col-manager-title">Toggle Columns</div>
                                <div className="col-manager-actions">
                                    <button
                                        type="button"
                                        className="col-manager-action"
                                        onClick={selectAllFixedColumns}
                                        disabled={selectedFixedColumnCount===fixedColumnKeys.length}
                                    >
                                        Select All
                                    </button>
                                    <button
                                        type="button"
                                        className="col-manager-action"
                                        onClick={deselectAllFixedColumns}
                                        disabled={selectedFixedColumnCount===0}
                                    >
                                        Deselect All
                                    </button>
                                </div>
                                <div className="col-manager-grid">{allColumns.map(col=>(<label key={col.key} className="col-toggle-item"><input type="checkbox" checked={visibleCols.has(col.key)} onChange={()=>toggleCol(col.key)}/><span>{col.label}{col.isDynamic&&<span style={{fontSize:9,marginLeft:4,background:"var(--accent-light)",color:"var(--accent)",padding:"1px 5px",borderRadius:3,fontWeight:600}}>NEW</span>}</span></label>))}</div>
                            </div>}
                        </div>
                    )}
                    {searchMode==="advanced"&&advancedResultCols&&(
                        <div className="adv-cols-info"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>{advancedResultCols.length} column{advancedResultCols.length!==1?"s":""} shown</div>
                    )}
                    <div className="export-wrap" ref={exportMenuRef}>
                        <button className="tool-btn tool-btn-primary" onClick={()=>!isExporting&&setShowExportMenu(p=>!p)} disabled={isExporting}>
                            <><svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>Export<svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><polyline points="6 9 12 15 18 9"/></svg></>
                        </button>
                        {showExportMenu&&<div className="export-dropdown">
                            <div className="export-scope-section"><div className="export-scope-header"><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><path d="M9 11l3 3L22 4"/><path d="M21 12v7a2 2 0 01-2 2H5a2 2 0 01-2-2V5a2 2 0 012-2h11"/></svg>Export Scope</div>
                            <div className="export-scope-tabs">{scopeTabs.map(s=>{
                                const isDisabled = s.key==="selected" && selectedRows.size===0;
                                return <button key={s.key} className={`export-scope-tab${exportScope===s.key?" export-scope-active":""}`} style={isDisabled?{opacity:0.4,cursor:"not-allowed",pointerEvents:"all"}:{}} onClick={()=>{ if(isDisabled){ showToast("No rows selected","error"); return; } setExportScope(s.key); }} title={isDisabled?"Select rows first":undefined}><span className="scope-tab-label">{s.label}</span><span className="scope-tab-count">{typeof s.count==="number"?s.count.toLocaleString():s.count}</span></button>;
                            })}</div></div>
                            <div className="export-section-block">
                                <div className="txn-export-scope-label">Export Section</div>
                                <div className="txn-export-scope-grid">
                                    {MAIN_EXPORT_TARGETS.map(target=>{
                                        const order = orderedExportTargets.indexOf(target.key) + 1;
                                        return (
                                        <button key={target.key} className={`txn-export-scope-btn${order?" active":""}`} onClick={()=>toggleExportTarget(target.key)}>
                                            <span>{target.label}</span>
                                            {order ? <span className="txn-export-order-badge">{order}</span> : null}
                                        </button>
                                    );})}
                                </div>
                            </div>
                            <div className="export-format-divider"><span>Format</span></div>
                            {mainExportFormats.map((option) => (
                                <button
                                    key={option.key}
                                    className="export-opt"
                                    onClick={()=>runExport(exportScope,orderedExportTargets,option.key)}
                                    disabled={option.disabled}
                                    title={option.disabledReason || undefined}
                                >
                                    <span className={`export-opt-icon ${option.iconClass}`}>{option.iconText}</span>
                                    <span className="export-opt-info">
                                        <span className="export-opt-name">{option.name}</span>
                                        <span className="export-opt-ext">{option.ext}</span>
                                    </span>
                                </button>
                            ))}
                        </div>}
                    </div>
                </div>

                {Object.keys(colFilters).some(k=>colFilters[k])&&<div className="active-filters-bar"><span className="af-label">Table filters:</span>{Object.entries(colFilters).filter(([,v])=>v).map(([k,v])=>(<span key={k} className="af-chip">{allColumns.find(c=>c.key===k)?.label}: {v}<button className="af-remove" onClick={()=>handleColFilter(k,"")}>&#x2715;</button></span>))}<button className="af-clear-all" onClick={()=>setColFilters({})}>Clear all</button></div>}

                <div className="table-wrapper" ref={tableWrapperRef} onScroll={syncScroll}>
                    <table style={{width:`calc(100% + ${extraWidth}px)`,minWidth:`calc(100% + ${extraWidth}px)`}}>
                        <thead><tr>
                            <th className="row-num-th">#</th>
                            <th className="ref-th">Reference</th>
                            {shownCols.map(col=>{
                                const cw=colWidths[col.key];
                                return (
                                    <th key={col.key} className={activeCol===col.key?"active-col":""} style={cw?{width:cw,minWidth:cw,maxWidth:cw}:{}} onClick={()=>setActiveCol(p=>p===col.key?null:col.key)}>
                                        <div className="th-label">
                                            <span className="th-text" onClick={e=>{e.stopPropagation();handleSort(col.key);}}>{col.label}{sortIcon(col.key)}</span>
                                            <span className="search-icon"><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round"><circle cx="11" cy="11" r="7"/><line x1="16.5" y1="16.5" x2="22" y2="22"/></svg></span>
                                        </div>
                                        <span className="col-resize-handle" title="Drag to resize. Double-click to reset."
                                            onMouseDown={e=>{const th=e.currentTarget.closest("th");handleColResizeStart(e,col.key,th);}}
                                            onDoubleClick={e=>{e.stopPropagation();resetColWidth(col.key);}}/>
                                        {activeCol===col.key&&<input className="col-search-input" placeholder={`Filter ${col.label}...`} value={colFilters[col.key]||""} onClick={e=>e.stopPropagation()} onChange={e=>handleColFilter(col.key,e.target.value)} autoFocus/>}
                                    </th>
                                );
                            })}
                        </tr></thead>
                        <tbody>
                            {currentRecords.length>0?currentRecords.map((msg,idx)=>{
                                const msgId=getMsgId(msg);
                                return(<tr key={msgId} className={selectedRows.has(msgId)?"row-selected":""} onClick={()=>toggleRow(msgId)}>
                                    <td className="row-num-td">{indexOfFirst+idx+1}</td>
                                    <td className="ref-td"><button className="ref-link" onClick={e=>openModal(msg,e,idx)}>{getReference(msg)}</button></td>
                                    {shownCols.map(col=>(<td key={col.key}>{renderCell(col,msg)}</td>))}
                                </tr>);
                            }):(
                                <tr><td colSpan={shownCols.length+2} className="no-result"><div className="no-result-inner"><svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="var(--gray-4)" strokeWidth="1.5"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg><p>No messages found</p><span>Try adjusting your search criteria</span></div></td></tr>
                            )}
                        </tbody>
                    </table>
                </div>
                <div className="bottom-scrollbar" ref={bottomScrollRef} onScroll={syncScroll}><div className="scroll-inner" style={{width:`calc(100% + ${extraWidth}px)`}}/></div>

                {totalPages>=1&&<div className="pagination-bar">
                    <div className="pagination-left"><span className="record-range">Showing <strong>{indexOfFirst+1}&ndash;{Math.min(indexOfFirst+currentRecords.length,serverTotal)}</strong> of <strong>{serverTotal.toLocaleString()}</strong> records</span></div>
                    <div className="pagination-center">
                        <button className="pg-btn pg-edge" onClick={()=>handlePageClick(1)} disabled={currentPage===1}>&#xAB;&#xAB;</button>
                        <button className="pg-btn" onClick={()=>handlePageClick(Math.max(1,currentPage-1))} disabled={currentPage===1}>&#x2039; Prev</button>
                        {startPage>1&&<span className="pg-ellipsis">…</span>}
                        {[...Array(pagesPerGroup)].map((_,i)=>{ const p=startPage+i; if(p>totalPages)return null; return <button key={p} className={`pg-btn pg-num${currentPage===p?" pg-active":""}`} onClick={()=>handlePageClick(p)}>{p}</button>; })}
                        {startPage+pagesPerGroup-1<totalPages&&<span className="pg-ellipsis">…</span>}
                        <button className="pg-btn" onClick={()=>handlePageClick(Math.min(totalPages,currentPage+1))} disabled={currentPage===totalPages}>Next &#x203A;</button>
                        <button className="pg-btn pg-edge" onClick={()=>handlePageClick(totalPages)} disabled={currentPage===totalPages}>&#xBB;&#xBB;</button>
                    </div>
                    <div className="pagination-right">
                        <label className="pg-label">Go to</label>
                        <input className="pg-goto" type="number" min="1" max={totalPages} value={goToPage} placeholder="pg" onChange={e=>setGoToPage(e.target.value)} onKeyDown={e=>{ if(e.key==="Enter"){const p=parseInt(goToPage);if(p>=1&&p<=totalPages)handlePageClick(p);setGoToPage("");}}}/>
                        <span className="pg-of-total">of {totalPages}</span><span className="pg-divider"/>
                        <label className="pg-label">Rows</label>
                        <select
                            className="pg-rows-select"
                            value={recordsPerPage}
                            onChange={e=>{
                                const nextSize = Number(e.target.value);
                                setRecordsPerPage(nextSize);
                                setCurrentPage(1);
                                setStartPage(1);
                                if (showResult) handleSearch(0, nextSize);
                            }}
                        >
                            <option value={10}>10</option><option value={20}>20</option><option value={50}>50</option><option value={100}>100</option>
                        </select>
                    </div>
                </div>}
            </>)}

            {/* ── Multi-Window Floating Modals ── */}
            {openModals.length > 0 && (
                <div className="fm-layer">
                    {openModals.map(modal => (
                        <FloatingModal key={modal.id} modal={modal} processed={processed}
                            onClose={closeModal} onBringToFront={bringToFront} onPatch={patchModal}
                            onPrev={goModalPrev} onNext={goModalNext}
                            getDisplayFormat={getDisplayFormat} getDisplayType={getDisplayType}
                            statusCls={statusCls} dirClass={dirClass} formatDirection={formatDirection}
                            token={token} onNotify={showToast}
                        />
                    ))}
                    {openModals.length > 1 && (
                        <button className="fm-close-all" onClick={closeAllModals}>
                            <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
                            Close all ({openModals.length} open)
                        </button>
                    )}
                </div>
            )}
        </div>
    );
}

export default Search;
