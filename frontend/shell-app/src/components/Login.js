import React, { useState } from "react";
import { useAuth } from "../AuthContext";
import BrandLogo from "./BrandLogo";

const API_BASE = process.env.REACT_APP_API_BASE_URL;

export default function Login({ appName = "SWIFT Platform", appSubtitle = "Financial Messaging Intelligence" }) {
  const { login } = useAuth();
  const [loginMode,   setLoginMode]   = useState("EMPLOYEE");
  const [employeeId,  setEmployeeId]  = useState("");
  const [password,    setPassword]    = useState("");
  const [error,       setError]       = useState("");
  const [isLoading,   setIsLoading]   = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError("");
    if (!employeeId.trim() || !password.trim()) { setError("Please fill in all fields."); return; }
    setIsLoading(true);
    try {
      const res  = await fetch(`${API_BASE}/api/auth/login`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ employeeId: employeeId.trim(), password, loginMode }),
      });
      const data = await res.json();
      if (!res.ok) { setError(data.message || "Invalid credentials. Please try again."); return; }
      login(data);
    } catch {
      setError("Unable to connect to server. Please try again.");
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="login-root">
      <div className="login-bg-pattern" />
      <div className="login-card">
        <div className="login-header">
          <div className="login-logo" style={{background:"transparent", padding:0, overflow:"hidden"}}>
            <BrandLogo variant="login" />
          </div>
          <h1 className="login-title">{appName}</h1>
          <p className="login-subtitle">{appSubtitle}</p>
        </div>

        <div className="login-toggle">
          {["EMPLOYEE", "ADMIN"].map(mode => (
            <button
              key={mode}
              type="button"
              className={`login-toggle-btn ${loginMode === mode ? "active" : ""}`}
              onClick={() => { setLoginMode(mode); setError(""); }}
            >
              {mode === "EMPLOYEE" ? "Employee" : "Admin"}
            </button>
          ))}
        </div>

        <form className="login-form" onSubmit={handleSubmit}>
          <div className="login-field">
            <label>Employee ID</label>
            <input
              type="text"
              placeholder="e.g. EMP001"
              value={employeeId}
              onChange={e => setEmployeeId(e.target.value)}
              autoComplete="username"
              autoFocus
            />
          </div>
          <div className="login-field">
            <label>Password</label>
            <input
              type="password"
              placeholder="Enter your password"
              value={password}
              onChange={e => setPassword(e.target.value)}
              autoComplete="current-password"
            />
          </div>
          {error && <div className="login-error">{error}</div>}
          <button type="submit" className="login-submit" disabled={isLoading}>
            {isLoading
              ? <><span className="login-spinner" />Signing in…</>
              : `Sign in as ${loginMode === "EMPLOYEE" ? "Employee" : "Admin"}`
            }
          </button>
        </form>
      </div>
    </div>
  );
}