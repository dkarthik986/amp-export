import React, { createContext, useContext, useState, useEffect, useCallback, useRef } from "react";
const AuthContext = createContext(null);
const API_BASE = process.env.REACT_APP_API_BASE_URL;
function decodeJwt(t) { try { return JSON.parse(atob(t.split(".")[1].replace(/-/g,"+").replace(/_/g,"/"))); } catch { return null; } }
function getExpiry(t) { const p=decodeJwt(t); return p?.exp ? p.exp*1000 : null; }
function isExpired(t) { const e=getExpiry(t); return !e||Date.now()>=e; }
export function AuthProvider({ children }) {
  const [user,setUser]=useState(null); const [token,setToken]=useState(null); const [loading,setLoading]=useState(true);
  const timerRef=useRef(null);
  const logout=useCallback(()=>{ clearTimeout(timerRef.current); timerRef.current=null; setToken(null); setUser(null); localStorage.removeItem("token"); localStorage.removeItem("user"); },[]);
  const scheduleAutoLogout=useCallback((jwt)=>{ clearTimeout(timerRef.current); const ms=(getExpiry(jwt)||0)-Date.now(); if(ms<=0){logout();return;} timerRef.current=setTimeout(logout,ms); },[logout]);
  useEffect(()=>{ const st=localStorage.getItem("token"); const su=localStorage.getItem("user"); if(st&&su){ if(isExpired(st)){localStorage.removeItem("token");localStorage.removeItem("user");}else{try{setToken(st);setUser(JSON.parse(su));scheduleAutoLogout(st);}catch{localStorage.clear();}}} setLoading(false); return()=>clearTimeout(timerRef.current); },[]);
  const login=useCallback((data)=>{ const u={employeeId:data.employeeId,name:data.name,role:data.role,email:data.email||""}; setToken(data.token); setUser(u); localStorage.setItem("token",data.token); localStorage.setItem("user",JSON.stringify(u)); scheduleAutoLogout(data.token); },[scheduleAutoLogout]);
  const refreshUser=useCallback(async()=>{ const st=localStorage.getItem("token"); if(!st||isExpired(st)){logout();return;} try{ const r=await fetch(`${API_BASE}/api/auth/me`,{headers:{Authorization:`Bearer ${st}`}}); if(!r.ok)throw new Error(); const j=await r.json(); const f=j.data||j; if(f?.employeeId){const u={...user,...f};setUser(u);localStorage.setItem("user",JSON.stringify(u));} }catch{logout();} },[user,logout]);
  return <AuthContext.Provider value={{user,token,login,logout,refreshUser,loading}}>{children}</AuthContext.Provider>;
}
export function useAuth(){ const c=useContext(AuthContext); if(!c)throw new Error("useAuth must be inside AuthProvider"); return c; }
export default AuthContext;
