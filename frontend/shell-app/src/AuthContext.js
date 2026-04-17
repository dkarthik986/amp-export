import React, {
  createContext, useContext, useState, useEffect, useCallback, useRef,
} from "react";

const AuthContext = createContext(null);
const API_BASE    = process.env.REACT_APP_API_BASE_URL;

// ── JWT helpers ───────────────────────────────────────────────────────────────
function decodeJwt(token) {
  try {
    return JSON.parse(atob(token.split(".")[1].replace(/-/g, "+").replace(/_/g, "/")));
  } catch { return null; }
}
function getExpiry(token)  { const p = decodeJwt(token); return p?.exp ? p.exp * 1000 : null; }
function isExpired(token)  { const e = getExpiry(token); return !e || Date.now() >= e; }

// ── Provider ──────────────────────────────────────────────────────────────────
export function AuthProvider({ children }) {
  const [user,    setUser]    = useState(null);
  const [token,   setToken]   = useState(null);
  const [loading, setLoading] = useState(true);
  const timerRef = useRef(null);

  const logout = useCallback(() => {
    clearTimeout(timerRef.current);
    timerRef.current = null;
    setToken(null);
    setUser(null);
    localStorage.removeItem("token");
    localStorage.removeItem("user");
  }, []);

  const scheduleAutoLogout = useCallback((jwt) => {
    clearTimeout(timerRef.current);
    const ms = (getExpiry(jwt) || 0) - Date.now();
    if (ms <= 0) { logout(); return; }
    timerRef.current = setTimeout(logout, ms);
  }, [logout]);

  // Restore session on mount
  useEffect(() => {
    const storedToken = localStorage.getItem("token");
    const storedUser  = localStorage.getItem("user");
    if (storedToken && storedUser) {
      if (isExpired(storedToken)) {
        localStorage.removeItem("token");
        localStorage.removeItem("user");
      } else {
        try {
          setToken(storedToken);
          setUser(JSON.parse(storedUser));
          scheduleAutoLogout(storedToken);
        } catch { localStorage.clear(); }
      }
    }
    setLoading(false);
    return () => clearTimeout(timerRef.current);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const login = useCallback((data) => {
    const userData = {
      employeeId: data.employeeId,
      name:       data.name,
      role:       data.role,
      email:      data.email || "",
    };
    setToken(data.token);
    setUser(userData);
    localStorage.setItem("token", data.token);
    localStorage.setItem("user",  JSON.stringify(userData));
    scheduleAutoLogout(data.token);
  }, [scheduleAutoLogout]);

  const refreshUser = useCallback(async () => {
    const stored = localStorage.getItem("token");
    if (!stored || isExpired(stored)) { logout(); return; }
    try {
      const res  = await fetch(`${API_BASE}/api/auth/me`, {
        headers: { Authorization: `Bearer ${stored}` },
      });
      if (!res.ok) throw new Error("Token invalid");
      const json  = await res.json();
      const fresh = json.data || json;
      if (fresh?.employeeId) {
        const updated = { ...user, ...fresh };
        setUser(updated);
        localStorage.setItem("user", JSON.stringify(updated));
      }
    } catch { logout(); }
  }, [user, logout]);

  return (
    <AuthContext.Provider value={{ user, token, login, logout, refreshUser, loading }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used inside <AuthProvider>");
  return ctx;
}

export default AuthContext;
