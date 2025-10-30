import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js";
import {
  getAuth,
  createUserWithEmailAndPassword,
  signInWithEmailAndPassword,
  onAuthStateChanged,
  signOut
} from "https://www.gstatic.com/firebasejs/10.12.2/firebase-auth.js";
import { getFunctions, httpsCallable as httpsCallableImport } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-functions.js";

// Firebase config (your project)
const firebaseConfig = {
  apiKey: "AIzaSyAJHujG0WiDj_4xN2KCHZL9A_GOE8TsC_k",
  authDomain: "trade-socket.firebaseapp.com",
  projectId: "trade-socket",
  storageBucket: "trade-socket.firebasestorage.app",
  messagingSenderId: "99661261931",
  appId: "1:99661261931:web:9ff05e4fda848bb077d52e"
};

document.addEventListener("DOMContentLoaded", () => {
  // Inject CSS to force dropdown visibility
  const style = document.createElement('style');
  style.textContent = `
    #wallet-select,
    #wallet-select option {
      color: white !important;
      background-color: #374151 !important;
    }
    #wallet-select {
      background-color: #374151 !important;
    }
  `;
  document.head.appendChild(style);
  
  const app = initializeApp(firebaseConfig);
  const functions = getFunctions(app, "us-west1");
     window.functions = functions; // Make globally accessible // region must match backend
     window.httpsCallable = httpsCallableImport; // Make globally accessible
  const auth = getAuth(app);
     window.auth = auth; // Make globally accessible for dashboard
  let currentUserId = null;

  // Toasts
  const showToast = (message, type = "success") => {
    const toastContainer = document.getElementById("toast-container");
    const toast = document.createElement("div");
    toast.textContent = message;
    toast.className = `toast ${type}`;
    toastContainer.appendChild(toast);
    setTimeout(() => toast.classList.add("show"), 10);
    setTimeout(() => {
      toast.classList.remove("show");
      setTimeout(() => toast.remove(), 500);
    }, 3000);
  };
     window.showToast = showToast; // Make globally accessible

  // Helpers
  const esc = (s) => {
    if (s === null || s === undefined) return "";
    // Important: This prevents XSS by escaping HTML characters.
    const map = {
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;"
    };
    return String(s).replace(/[&<>"']/g, (c) => map[c]);
  };
     window.esc = esc; // Make globally accessible
  const fmtMoney = (n) => {
    const x = Number(n || 0);
    return isFinite(x) ? "$" + x.toFixed(2) : "—";
  };
  const fmtNum = (n) => {
    const x = Number(n || 0);
    return isFinite(x) ? x.toFixed(2) : "—";
  };
     const fmtDate = (dateStr) => {
       if (!dateStr) return "—";
       try {
         // Handle both ISO strings and Date objects
         let d;
         if (dateStr instanceof Date) {
           d = dateStr;
         } else if (typeof dateStr === 'string') {
           // If it's already in YYYY-MM-DD format, append time to avoid timezone issues
           if (/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
             d = new Date(dateStr + 'T00:00:00');
           } else {
             d = new Date(dateStr);
           }
         } else {
           return String(dateStr);
         }
         
         if (isNaN(d.getTime())) {
           console.warn('Invalid date:', dateStr);
           return String(dateStr);
         }
         
         return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
       } catch (e) {
         console.error('Date formatting error:', e, dateStr);
         return String(dateStr);
       }
     };
   
  // ===== Views (Auth) =====
  const loginView = document.getElementById("login-view");
  const registerView = document.getElementById("register-view");
  const appView = document.getElementById("app-view");
  const registerForm = document.getElementById("register-form");
  const loginForm = document.getElementById("login-form");
  const userEmailSpan = document.getElementById("user-email");

  document.getElementById("show-register").addEventListener("click", (e) => {
    e.preventDefault();
    loginView.classList.remove("active");
    registerView.classList.add("active");
  });
  document.getElementById("show-login").addEventListener("click", (e) => {
    e.preventDefault();
    registerView.classList.remove("active");
    loginView.classList.add("active");
  });

  registerForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    const email = e.target["register-email"].value;
    const password = e.target["register-password"].value;
    const button = e.target.querySelector("button");
    button.disabled = true;
    const errEl = document.getElementById("register-error");
    errEl.textContent = "";
    try {
      await createUserWithEmailAndPassword(auth, email, password);
      window.showToast("Account created successfully! Please log in.", "success");
      registerView.classList.remove("active");
      loginView.classList.add("active");
    } catch (error) {
      errEl.textContent = error.message;
    } finally {
      button.disabled = false;
    }
  });

  loginForm.addEventListener("submit", (e) => {
    e.preventDefault();
    const email = e.target.email.value;
    const password = e.target.password.value;
    const errEl = document.getElementById("auth-error");
    errEl.textContent = "";
    signInWithEmailAndPassword(auth, email, password).catch((error) => {
      errEl.textContent = error.message;
    });
  });

  document.getElementById("logout-btn").addEventListener("click", () => {
    signOut(auth).catch((error) => console.error("Logout failed:", error));
  });

  // ===== Tab switching =====
  const showTab = (tabName) => {
    document.querySelectorAll(".tab-content").forEach(tab => tab.classList.remove("active"));
    document.querySelectorAll(".tab-btn").forEach(btn => {
      btn.classList.remove("text-cyan-400", "border-cyan-400");
      btn.classList.add("text-gray-400");
    });

    const target = document.getElementById(`${tabName}-content`);
    if (target) target.classList.add("active");
    const activeBtn = document.querySelector(`.tab-btn[onclick*="'${tabName}'"]`);
    if (activeBtn) {
      activeBtn.classList.add("text-cyan-400", "border-cyan-400");
      activeBtn.classList.remove("text-gray-400");
    }

    // Automatically refresh the wallets list when the tab is viewed.
    if (tabName === "wallets") {
      listWallets();
    }
      
      // Initialize KPI tab when shown
      if (tabName === "kpi") {
        if (typeof populateKPIWalletDropdown === 'function') {
          populateKPIWalletDropdown();
        }
      }
       
       // Initialize backfill tab when shown
       if (tabName === "backfill") {
         initBackfillTab();
       }
          
          // Initialize dashboard tab when shown
          if (tabName === "dashboard") {
            initDashboard();
            refreshDashboard();
          }
             
             // Initialize settings tab when shown
             if (tabName === "settings") {
               initSettings();
             }
  };
  window.showTab = showTab;

  // ===== Controls — Wallet picker + settings + snapshot + run now =====
  let walletsCache = [];
  let selectedWalletId = null;
     window.selectedWalletId = selectedWalletId; // Make accessible to backfill functions
  let baselinesCache = {}; // Store baselines by symbol

  const runLoopToggle = document.getElementById("run-loop");
  const tradingWindowSel = document.getElementById("trading-window");
  const customStartInput = document.getElementById("custom-start-time");
  const customEndInput = document.getElementById("custom-end-time");
  const cooldownInput = document.getElementById("cooldown");
  const saveGlobalBtn = document.getElementById("save-global-settings");

  // Snapshot spans (filled after injection)
  let snapEquityEl, snapPositionsEl, snapCashEl, refreshSnapshotBtn, runNowBtn, computeBaselinesBtn, baselineStatusEl;

  function ensureWalletPicker() {
    const controlsRoot = document.getElementById("controls-content");
    if (!controlsRoot) return null;
    if (document.getElementById("wallet-picker")) return document.getElementById("wallet-picker");

    const card = document.createElement("div");
    card.className = "bg-gray-800 p-4 rounded-lg shadow-lg mb-6";
    card.innerHTML = `
      <div class="flex items-start gap-4 flex-wrap" id="wallet-picker">
        <div class="flex-1 min-w-[260px]">
          <label for="wallet-select" class="block text-sm font-medium text-gray-400">Select Wallet</label>
          <select id="wallet-select" class="bg-gray-700 border border-gray-600 text-white text-sm rounded-lg focus:ring-cyan-500 focus:border-cyan-500 block w-full p-2.5" style="color: white !important;">
            <option value="" style="color: white; background-color: #374151;">Loading...</option>
          </select>
        </div>
        <div class="flex items-end gap-2">
          <button id="refresh-wallets" class="bg-gray-700 hover:bg-gray-600 px-3 py-2 rounded">Refresh</button>
        </div>
      </div>

      <div id="wallet-snapshot" class="mt-4 grid grid-cols-1 md:grid-cols-4 gap-3">
        <div class="bg-gray-900 border border-gray-700 rounded p-3">
          <div class="text-xs text-gray-400">Equity</div>
          <div id="snap-equity" class="text-lg font-semibold text-cyan-400">—</div>
        </div>
        <div class="bg-gray-900 border border-gray-700 rounded p-3">
          <div class="text-xs text-gray-400">Positions</div>
          <div id="snap-positions" class="text-lg font-semibold text-cyan-400">—</div>
        </div>
        <div class="bg-gray-900 border border-gray-700 rounded p-3">
          <div class="text-xs text-gray-400">Cash</div>
          <div id="snap-cash" class="text-lg font-semibold text-cyan-400">—</div>
        </div>
        <div class="flex items-center gap-2">
          <button id="refresh-snapshot" class="bg-gray-700 hover:bg-gray-600 px-3 py-2 rounded w-full">Refresh Snapshot</button>
        </div>
      </div>

      <div class="mt-4 flex flex-wrap gap-2">
        <button id="compute-baselines-btn" class="bg-purple-600 hover:bg-purple-700 px-4 py-2 rounded text-white font-medium">
          Compute Baselines
        </button>
        <button id="run-now-btn" class="bg-cyan-600 hover:bg-cyan-700 px-4 py-2 rounded text-white font-medium">
          Run Now (Dry Run)
        </button>
        <div id="baseline-status" class="flex items-center text-sm text-gray-400 ml-2">
          <span>—</span>
        </div>
      </div>
    `;
    const saveKeysBtn = document.getElementById("save-api-keys");
    if (saveKeysBtn && saveKeysBtn.closest(".bg-gray-800")) {
      controlsRoot.insertBefore(card, saveKeysBtn.closest(".bg-gray-800"));
    } else {
      controlsRoot.prepend(card);
    }

    // Wire events
    const walletSelect = document.getElementById("wallet-select");
    walletSelect.addEventListener("change", async (e) => {
      selectedWalletId = e.target.value || null; window.selectedWalletId = selectedWalletId;
      if (selectedWalletId) {
        localStorage.setItem("lastSelectedWalletId", selectedWalletId);
        await loadWalletSettings(selectedWalletId);
        await loadSymbolsForWallet(selectedWalletId);
        await refreshSnapshot();
        await loadBaselinesForWallet(selectedWalletId);
      }
    });

    document.getElementById("refresh-wallets").addEventListener("click", async () => {
      await loadWalletsIntoSelect();
    });

    snapEquityEl = document.getElementById("snap-equity");
    snapPositionsEl = document.getElementById("snap-positions");
    snapCashEl = document.getElementById("snap-cash");
    refreshSnapshotBtn = document.getElementById("refresh-snapshot");
    runNowBtn = document.getElementById("run-now-btn");
    computeBaselinesBtn = document.getElementById("compute-baselines-btn");
    baselineStatusEl = document.getElementById("baseline-status");

    refreshSnapshotBtn.addEventListener("click", refreshSnapshot);
    runNowBtn.addEventListener("click", runNowDryRun);
    computeBaselinesBtn.addEventListener("click", computeBaselines);

    return document.getElementById("wallet-picker");
  }

  async function loadWalletsIntoSelect() {
    if (!currentUserId) return;
    try {
      const fn = window.httpsCallable(window.functions, "listWallets");
      const result = await fn();
      walletsCache = result.data.wallets || [];
      const sel = document.getElementById("wallet-select");
      if (!sel) return;
      if (!walletsCache.length) {
        sel.innerHTML = `<option value="" style="color: white; background-color: #374151;">No wallets yet</option>`;
        selectedWalletId = null; window.selectedWalletId = selectedWalletId;
        return;
      }
      sel.innerHTML = walletsCache.map(w => {
        const label = `${esc(w.name)} (${esc(w.env)})`;
        return `<option value="${esc(w.wallet_id)}" style="color: white; background-color: #374151;">${label}</option>`;
      }).join("");

      const lastId = localStorage.getItem("lastSelectedWalletId");
      if (lastId && walletsCache.some(w => w.wallet_id === lastId)) {
        sel.value = lastId;
        selectedWalletId = lastId; window.selectedWalletId = selectedWalletId;
      } else {
        sel.value = walletsCache[0].wallet_id;
        selectedWalletId = walletsCache[0].wallet_id; window.selectedWalletId = selectedWalletId;
      }
      if (selectedWalletId) {
        await loadWalletSettings(selectedWalletId);
        await loadSymbolsForWallet(selectedWalletId);
        await refreshSnapshot();
        await loadBaselinesForWallet(selectedWalletId);
      }
    } catch (e) {
      console.error("loadWalletsIntoSelect error:", e);
      window.showToast("Failed to load wallets", "error");
    }
  }

  async function loadWalletSettings(walletId) {
    if (!walletId) return;
    try {
      const fn = window.httpsCallable(window.functions, "listWallets");
      const result = await fn();
      const wallet = (result.data.wallets || []).find(w => w.wallet_id === walletId);
      if (!wallet) return;
      if (runLoopToggle) runLoopToggle.checked = !!wallet.enabled;
      if (tradingWindowSel) tradingWindowSel.value = wallet.trading_window || "RTH";
      if (customStartInput) customStartInput.value = wallet.custom_start_et || "";
      if (customEndInput) customEndInput.value = wallet.custom_end_et || "";
      if (cooldownInput) cooldownInput.value = wallet.cooldown_min || 0;
    } catch (e) {
      console.error("loadWalletSettings error:", e);
    }
  }

  async function refreshSnapshot() {
    if (!window.selectedWalletId) return;
    if (snapEquityEl) snapEquityEl.textContent = "Loading...";
    if (snapPositionsEl) snapPositionsEl.textContent = "Loading...";
    if (snapCashEl) snapCashEl.textContent = "Loading...";
    try {
      const fn = window.httpsCallable(window.functions, "getWalletSnapshot");
      const result = await fn({ walletId: window.selectedWalletId });
      // Backend returns { success: true, snapshot: { equity, positionsValue, cash, ... } }
      const snapshot = result.data.snapshot || result.data;
      const { equity, positionsValue, cash } = snapshot || {};
      if (snapEquityEl) snapEquityEl.textContent = fmtMoney(equity);
      if (snapPositionsEl) snapPositionsEl.textContent = fmtMoney(positionsValue);
      if (snapCashEl) snapCashEl.textContent = fmtMoney(cash);
    } catch (e) {
      console.error("refreshSnapshot error:", e);
      console.error("Error details:", e.message, e.code);
      if (snapEquityEl) snapEquityEl.textContent = "Error";
      if (snapPositionsEl) snapPositionsEl.textContent = "Error";
      if (snapCashEl) snapCashEl.textContent = "Error";
      // Show more specific error message
      if (e.message && e.message.includes("keys not set")) {
        window.showToast("Please set your Alpaca API keys first", "error");
      } else {
        window.showToast("Failed to load snapshot: " + (e.message || "Unknown error"), "error");
      }
    }
  }

  async function runNowDryRun() {
    if (!window.selectedWalletId) {
      window.showToast("Select a wallet first", "error");
      return;
    }
    if (runNowBtn) runNowBtn.disabled = true;
    try {
      const fn = window.httpsCallable(window.functions, "runWalletOnce");
      const result = await fn({ walletId: window.selectedWalletId });
      console.log("Dry run result:", result.data);
      window.showToast("Dry run completed. Check console for details.", "success");
    } catch (e) {
      console.error("runNowDryRun error:", e);
      window.showToast(`Dry run failed: ${e.message}`, "error");
    } finally {
      if (runNowBtn) runNowBtn.disabled = false;
    }
  }

  async function computeBaselines() {
    if (!window.selectedWalletId) {
      window.showToast("Select a wallet first", "error");
      return;
    }
    if (computeBaselinesBtn) computeBaselinesBtn.disabled = true;
    if (baselineStatusEl) baselineStatusEl.innerHTML = '<span class="text-yellow-400">Computing...</span>';
    try {
      const fn = window.httpsCallable(window.functions, "computeWalletBaselines");
      await fn({ walletId: window.selectedWalletId });
      window.showToast("Baselines computed successfully", "success");
      await loadBaselinesForWallet(selectedWalletId);
      await loadSymbolsForWallet(selectedWalletId); // Refresh to show new baseline data
    } catch (e) {
      console.error("computeBaselines error:", e);
      window.showToast(`Failed to compute baselines: ${e.message}`, "error");
      if (baselineStatusEl) baselineStatusEl.innerHTML = '<span class="text-red-400">Error</span>';
    } finally {
      if (computeBaselinesBtn) computeBaselinesBtn.disabled = false;
    }
  }

  async function loadBaselinesForWallet(walletId) {
    if (!walletId) return;
    try {
      const fn = window.httpsCallable(window.functions, "getWalletBaselines");
      const result = await fn({ walletId });
      baselinesCache = result.data.baselines || {};
      
      // Update status display
      const dates = Object.values(baselinesCache).flatMap(sessions => 
        Object.values(sessions).map(b => b.as_of_date)
      ).filter(Boolean);
      
      if (dates.length > 0) {
        const latestDate = dates.sort().reverse()[0];
        const today = new Date().toISOString().split('T')[0];
        const yesterday = new Date(Date.now() - 86400000).toISOString().split('T')[0];
        
        let statusClass = "text-gray-400";
        let statusText = fmtDate(latestDate);
        
        if (latestDate === today) {
          statusClass = "text-green-400";
          statusText = "Today (" + fmtDate(latestDate) + ")";
        } else if (latestDate === yesterday) {
          statusClass = "text-yellow-400";
          statusText = "Yesterday (" + fmtDate(latestDate) + ")";
        } else {
          statusClass = "text-orange-400";
          statusText = "Stale (" + fmtDate(latestDate) + ")";
        }
        
        if (baselineStatusEl) {
          baselineStatusEl.innerHTML = `<span class="${statusClass}">Last baseline: ${statusText}</span>`;
        }
      } else {
        if (baselineStatusEl) {
          baselineStatusEl.innerHTML = '<span class="text-gray-400">No baselines computed</span>';
        }
      }
    } catch (e) {
      console.error("loadBaselinesForWallet error:", e);
      baselinesCache = {};
      if (baselineStatusEl) {
        baselineStatusEl.innerHTML = '<span class="text-red-400">Error loading baselines</span>';
      }
    }
  }

  if (saveGlobalBtn) {
    saveGlobalBtn.addEventListener("click", async () => {
      if (!window.selectedWalletId) {
        window.showToast("Select a wallet first", "error");
        return;
      }
      const enabled = runLoopToggle?.checked || false;
      const tradingWindow = tradingWindowSel?.value || "RTH";
      const customStart = customStartInput?.value || null;
      const customEnd = customEndInput?.value || null;
      const cooldown = parseInt(cooldownInput?.value || "0", 10);
      try {
        const fn = window.httpsCallable(window.functions, "updateWallet");
        await fn({
          walletId: window.selectedWalletId,
          enabled,
          tradingWindow,
          customStartEt: customStart,
          customEndEt: customEnd,
          cooldownMin: cooldown
        });
        window.showToast("Global settings saved", "success");
      } catch (e) {
        console.error("saveGlobalSettings error:", e);
        window.showToast("Failed to save settings", "error");
      }
    });
  }

  // ===== API Keys =====
  const polygonKeyInput = document.getElementById("polygon-key");
  const alpacaPaperKeyInput = document.getElementById("alpaca-paper-key");
  const alpacaPaperSecretInput = document.getElementById("alpaca-paper-secret");
  const saveApiKeysBtn = document.getElementById("save-api-keys");

  if (saveApiKeysBtn) {
    saveApiKeysBtn.addEventListener("click", async () => {
        if (!selectedWalletId) {
          window.showToast("Please select a wallet first", "error");
          return;
        }
      const polygonKey = polygonKeyInput?.value || "";
      const alpacaPaperKey = alpacaPaperKeyInput?.value || "";
      const alpacaPaperSecret = alpacaPaperSecretInput?.value || "";
      try {
        const fn = window.httpsCallable(window.functions, "saveWalletApiKeys");
        await fn({ walletId: selectedWalletId, polygonKey, alpacaPaperKey, alpacaPaperSecret });
        window.showToast("API keys saved to wallet", "success");
        // Reload keys to confirm they were saved
        await loadApiKeys();
        // Refresh snapshot to use new keys
        if (selectedWalletId) {
          await refreshSnapshot();
        }
      } catch (e) {
        console.error("saveApiKeys error:", e);
        window.showToast("Failed to save API keys", "error");
      }
    });
  }

  async function loadApiKeys() {
    if (!selectedWalletId) {
      console.log("No wallet selected, skipping API key load");
      return;
    }
    try {
      const fn = window.httpsCallable(window.functions, "loadWalletApiKeys");
      const result = await fn({ walletId: selectedWalletId });
      // Backend returns { success: true, data: { polygonKey, ... } }
      // Firebase wraps it, so we get result.data.data
      const keys = result.data.data || result.data;
      const { polygonKey, alpacaPaperKey, alpacaPaperSecret } = keys;
      if (polygonKeyInput) polygonKeyInput.value = polygonKey || "";
      if (alpacaPaperKeyInput) alpacaPaperKeyInput.value = alpacaPaperKey || "";
      if (alpacaPaperSecretInput) alpacaPaperSecretInput.value = alpacaPaperSecret || "";
      console.log("API keys loaded successfully for wallet:", selectedWalletId);
    } catch (e) {
      console.error("loadApiKeys error:", e);
    }
  }

  // ===== Symbols =====
  const METHODS = ["EQUAL_MEAN", "MEDIAN", "VWAP_RATIO", "VOL_WEIGHTED", "WINSORIZED"];
  let symbolsCache = [];

  const symbolsTableBody = document.getElementById("symbols-table-body");
  const symbolsCardsContainer = document.getElementById("symbols-cards");
  const symbolAddInput = document.getElementById("symbol-add");
  const budgetModeAddSelect = document.getElementById("budget-mode-add");
  const budgetAddInput = document.getElementById("budget-add");
  const percentAddInput = document.getElementById("percent-add");
  const buyRthAddInput = document.getElementById("buy-rth-add");
  const sellRthAddInput = document.getElementById("sell-rth-add");
  const methodRthAddSelect = document.getElementById("method-rth-add");
  const buyAhAddInput = document.getElementById("buy-ah-add");
  const sellAhAddInput = document.getElementById("sell-ah-add");
  const methodAhAddSelect = document.getElementById("method-ah-add");
  const addSymbolBtn = document.getElementById("add-symbol-btn");

  if (addSymbolBtn) {
    addSymbolBtn.addEventListener("click", upsertSymbolFromAddForm);
  }

  async function loadSymbolsForWallet(walletId) {
    if (!walletId) return;
    try {
      const fn = window.httpsCallable(window.functions, "listWalletSymbols");
      const result = await fn({ walletId });
      symbolsCache = result.data.symbols || [];
      if (window.innerWidth >= 768) renderSymbolsTable();
      else renderSymbolsCards();
    } catch (e) {
      console.error("loadSymbolsForWallet error:", e);
      window.showToast("Failed to load symbols", "error");
    }
  }

  window.addEventListener("resize", () => {
    if (!symbolsCache.length) return;
    if (window.innerWidth >= 768) renderSymbolsTable();
    else renderSymbolsCards();
  });

  if (symbolsTableBody) {
    symbolsTableBody.addEventListener("click", (e) => {
      const btn = e.target.closest("button");
      if (!btn) return;
      const act = btn.dataset.act;
      const sym = btn.dataset.sym;
      if (act === "save") saveSymbol(sym);
      else if (act === "del") deleteSymbol(sym);
    });
  }

  if (symbolsCardsContainer) {
    symbolsCardsContainer.addEventListener("click", (e) => {
      const btn = e.target.closest("button");
      if (!btn) return;
      const act = btn.dataset.act;
      const sym = btn.dataset.sym;
      if (act === "save") saveSymbol(sym);
      else if (act === "del") deleteSymbol(sym);
    });
  }

  function getBaselineInfo(symbol) {
    const baselines = baselinesCache[symbol] || {};
    const rth = baselines.RTH || {};
    const ah = baselines.AH || {};
    return {
      rthValue: rth.value || null,
      rthDate: rth.as_of_date || null,
      ahValue: ah.value || null,
      ahDate: ah.as_of_date || null
    };
  }

  function getBaselineFreshnessClass(dateStr) {
    if (!dateStr) return "text-gray-500";
    const today = new Date().toISOString().split('T')[0];
    const yesterday = new Date(Date.now() - 86400000).toISOString().split('T')[0];
    if (dateStr === today) return "text-green-400";
    if (dateStr === yesterday) return "text-yellow-400";
    return "text-orange-400";
  }

  function rowHtml(row) {
    const s = esc(row.symbol);
    const bm = (row.budget_mode || "fixed");
    const pb = Number(row.percent_budget ?? 0);
    const baseline = getBaselineInfo(row.symbol);
    
    const rthBaselineDisplay = baseline.rthValue !== null 
      ? `<span class="${getBaselineFreshnessClass(baseline.rthDate)}">${fmtNum(baseline.rthValue)}</span>`
      : '<span class="text-gray-500">—</span>';
    
    const ahBaselineDisplay = baseline.ahValue !== null 
      ? `<span class="${getBaselineFreshnessClass(baseline.ahDate)}">${fmtNum(baseline.ahValue)}</span>`
      : '<span class="text-gray-500">—</span>';
    
    return `
      <tr data-sym="${s}">
        <td class="table-cell font-semibold">${s}</td>
        <td class="table-cell">
          <select id="bm-${s}" class="bg-gray-700 border border-gray-600 rounded p-1">
            <option value="fixed"   ${bm === "fixed" ? "selected" : ""}>fixed</option>
            <option value="percent" ${bm === "percent" ? "selected" : ""}>percent</option>
          </select>
        </td>
        <td class="table-cell"><input type="number" step="50" class="bg-gray-700 border border-gray-600 rounded p-1 w-28" id="bud-${s}" value="${Number(row.buy_budget_usd ?? 0)}" /></td>
        <td class="table-cell"><input type="number" step="0.1" class="bg-gray-700 border border-gray-600 rounded p-1 w-24" id="pb-${s}" value="${pb}" /></td>
        <td class="table-cell"><input type="number" step="0.1" class="bg-gray-700 border border-gray-600 rounded p-1 w-20" id="bur-${s}" value="${Number(row.buy_pct_rth ?? 0)}" /></td>
        <td class="table-cell"><input type="number" step="0.1" class="bg-gray-700 border border-gray-600 rounded p-1 w-20" id="ser-${s}" value="${Number(row.sell_pct_rth ?? 0)}" /></td>
        <td class="table-cell">
          <select id="mer-${s}" class="bg-gray-700 border border-gray-600 rounded p-1">
            ${METHODS.map(m => `<option ${m === (row.method_rth || "EQUAL_MEAN") ? "selected" : ""}>${m}</option>`).join("")}
          </select>
        </td>
        <td class="table-cell">${rthBaselineDisplay}</td>
        <td class="table-cell"><input type="number" step="0.1" class="bg-gray-700 border border-gray-600 rounded p-1 w-20" id="bua-${s}" value="${Number(row.buy_pct_ah ?? 0)}" /></td>
        <td class="table-cell"><input type="number" step="0.1" class="bg-gray-700 border border-gray-600 rounded p-1 w-20" id="sea-${s}" value="${Number(row.sell_pct_ah ?? 0)}" /></td>
        <td class="table-cell">
          <select id="mea-${s}" class="bg-gray-700 border border-gray-600 rounded p-1">
            ${METHODS.map(m => `<option ${m === (row.method_ah || "EQUAL_MEAN") ? "selected" : ""}>${m}</option>`).join("")}
          </select>
        </td>
        <td class="table-cell">${ahBaselineDisplay}</td>
        <td class="table-cell">
          <button class="px-2 py-1 bg-gray-700 hover:bg-gray-600 rounded mr-2 text-xs" data-act="save" data-sym="${s}">Save</button>
          <button class="px-2 py-1 bg-red-600 hover:bg-red-700 rounded text-xs" data-act="del" data-sym="${s}">Delete</button>
        </td>
      </tr>
    `;
  }

  function cardHtml(row) {
    const s = esc(row.symbol);
    const bm = (row.budget_mode || "fixed");
    const pb = Number(row.percent_budget ?? 0);
    const baseline = getBaselineInfo(row.symbol);
    
    const rthBaselineDisplay = baseline.rthValue !== null 
      ? `<span class="${getBaselineFreshnessClass(baseline.rthDate)}">${fmtNum(baseline.rthValue)}</span> <span class="text-xs text-gray-500">(${fmtDate(baseline.rthDate)})</span>`
      : '<span class="text-gray-500">Not computed</span>';
    
    const ahBaselineDisplay = baseline.ahValue !== null 
      ? `<span class="${getBaselineFreshnessClass(baseline.ahDate)}">${fmtNum(baseline.ahValue)}</span> <span class="text-xs text-gray-500">(${fmtDate(baseline.ahDate)})</span>`
      : '<span class="text-gray-500">Not computed</span>';
    
    return `
      <div class="bg-gray-900 border border-gray-700 rounded p-3" data-sym="${s}">
        <div class="text-cyan-400 font-semibold mb-2">${s}</div>
        
        <div class="mb-3 p-2 bg-gray-800 rounded">
          <div class="text-xs text-gray-400 mb-1">RTH Baseline</div>
          <div class="text-sm">${rthBaselineDisplay}</div>
        </div>
        
        <div class="mb-3 p-2 bg-gray-800 rounded">
          <div class="text-xs text-gray-400 mb-1">Stock Price</div>
          <div class="text-sm">${ahBaselineDisplay}</div>
        </div>
        
        <div class="grid grid-cols-2 gap-3">
          <label class="text-xs text-gray-400">Budget Type
            <select id="bm-${s}" class="mt-1 bg-gray-700 border border-gray-600 rounded p-1 w-full">
              <option value="fixed"   ${bm === "fixed" ? "selected" : ""}>fixed</option>
              <option value="percent" ${bm === "percent" ? "selected" : ""}>percent</option>
            </select>
          </label>
          <label class="text-xs text-gray-400">Budget ($)
            <input type="number" step="50" class="mt-1 bg-gray-700 border border-gray-600 rounded p-1 w-full" id="bud-${s}" value="${Number(row.buy_budget_usd ?? 0)}" />
          </label>
          <label class="text-xs text-gray-400">% of Equity
            <input type="number" step="0.1" class="mt-1 bg-gray-700 border border-gray-600 rounded p-1 w-full" id="pb-${s}" value="${pb}" />
          </label>
          <label class="text-xs text-gray-400">Buy % RTH
            <input type="number" step="0.1" class="mt-1 bg-gray-700 border border-gray-600 rounded p-1 w-full" id="bur-${s}" value="${Number(row.buy_pct_rth ?? 0)}" />
          </label>
          <label class="text-xs text-gray-400">Sell % RTH
            <input type="number" step="0.1" class="mt-1 bg-gray-700 border border-gray-600 rounded p-1 w-full" id="ser-${s}" value="${Number(row.sell_pct_rth ?? 0)}" />
          </label>
          <label class="text-xs text-gray-400">Method RTH
            <select id="mer-${s}" class="mt-1 bg-gray-700 border border-gray-600 rounded p-1 w-full">
              ${METHODS.map(m => `<option ${m === (row.method_rth || "EQUAL_MEAN") ? "selected" : ""}>${m}</option>`).join("")}
            </select>
          </label>
          <label class="text-xs text-gray-400">Buy % AH
            <input type="number" step="0.1" class="mt-1 bg-gray-700 border border-gray-600 rounded p-1 w-full" id="bua-${s}" value="${Number(row.buy_pct_ah ?? 0)}" />
          </label>
          <label class="text-xs text-gray-400">Sell % AH
            <input type="number" step="0.1" class="mt-1 bg-gray-700 border border-gray-600 rounded p-1 w-full" id="sea-${s}" value="${Number(row.sell_pct_ah ?? 0)}" />
          </label>
          <label class="text-xs text-gray-400">Method AH
            <select id="mea-${s}" class="mt-1 bg-gray-700 border border-gray-600 rounded p-1 w-full">
              ${METHODS.map(m => `<option ${m === (row.method_ah || "EQUAL_MEAN") ? "selected" : ""}>${m}</option>`).join("")}
            </select>
          </label>
        </div>
        <div class="flex justify-end gap-2 mt-3">
          <button class="px-2 py-1 bg-gray-700 hover:bg-gray-600 rounded text-xs" data-act="save" data-sym="${s}">Save</button>
          <button class="px-2 py-1 bg-red-600 hover:bg-red-700 rounded text-xs" data-act="del" data-sym="${s}">Delete</button>
        </div>
      </div>
    `;
  }

  function renderSymbolsTable() {
    if (!symbolsTableBody) return;
    if (!symbolsCache.length) {
      symbolsTableBody.innerHTML = `<tr><td class="table-cell" colspan="13">No symbols for this wallet yet.</td></tr>`;
      return;
    }
    symbolsTableBody.innerHTML = symbolsCache.map(rowHtml).join("");
  }

  function renderSymbolsCards() {
    if (!symbolsCardsContainer) return;
    if (!symbolsCache.length) {
      symbolsCardsContainer.innerHTML = `<div class="text-gray-400 text-sm">No symbols for this wallet yet.</div>`;
      return;
    }
    symbolsCardsContainer.innerHTML = symbolsCache.map(cardHtml).join("");
  }

  async function upsertSymbolFromAddForm() {
    if (!window.selectedWalletId) { window.showToast("Select a wallet first", "error"); return; }
    const sym = (symbolAddInput?.value || "").toUpperCase().trim();
    if (!sym) { window.showToast("Enter a symbol", "error"); return; }
    const payload = {
      walletId: window.selectedWalletId,
      symbol: sym,
      buyBudgetUsd: parseFloat(budgetAddInput?.value || "0") || 0,
      budgetMode: (budgetModeAddSelect?.value || "fixed"),
      percentBudget: parseFloat(percentAddInput?.value || "0") || 0,
      buyPctRth: parseFloat(buyRthAddInput?.value || "0") || 0,
      sellPctRth: parseFloat(sellRthAddInput?.value || "0") || 0,
      buyPctAh: parseFloat(buyAhAddInput?.value || "0") || 0,
      sellPctAh: parseFloat(sellAhAddInput?.value || "0") || 0,
      methodRth: methodRthAddSelect?.value || "EQUAL_MEAN",
      methodAh: methodAhAddSelect?.value || "EQUAL_MEAN"
    };
    try {
      const fn = window.httpsCallable(window.functions, "upsertWalletSymbol");
      await fn(payload);
      window.showToast("Symbol saved", "success");
      await loadSymbolsForWallet(selectedWalletId);
      if (symbolAddInput) symbolAddInput.value = "";
    } catch (e) {
      console.error("upsertWalletSymbol error:", e);
      window.showToast("Failed to save symbol", "error");
    }
  }

  function gatherRowPayload(sym) {
    const getNum = (id) => parseFloat(document.getElementById(id)?.value || "0") || 0;
    const getSel = (id) => document.getElementById(id)?.value || "EQUAL_MEAN";
    const getStr = (id) => document.getElementById(id)?.value || "";
    return {
      walletId: window.selectedWalletId,
      symbol: sym,
      buyBudgetUsd: getNum(`bud-${sym}`),
      budgetMode: getStr(`bm-${sym}`),
      percentBudget: getNum(`pb-${sym}`),
      buyPctRth: getNum(`bur-${sym}`),
      sellPctRth: getNum(`ser-${sym}`),
      buyPctAh: getNum(`bua-${sym}`),
      sellPctAh: getNum(`sea-${sym}`),
      methodRth: getSel(`mer-${sym}`),
      methodAh: getSel(`mea-${sym}`)
    };
  }

  async function saveSymbol(sym) {
    if (!window.selectedWalletId) return;
    const payload = gatherRowPayload(sym);
    try {
      const fn = window.httpsCallable(window.functions, "upsertWalletSymbol");
      await fn(payload);
      window.showToast(`${sym} updated`, "success");
      await loadSymbolsForWallet(selectedWalletId);
    } catch (e) {
      console.error("saveSymbol error:", e);
      window.showToast(`Save failed: ${e.message}`, "error");
    }
  }

  async function deleteSymbol(sym) {
    if (!window.selectedWalletId) return;
    if (!window.confirm(`Delete ${sym} from this wallet?`)) return;
    try {
      const fn = window.httpsCallable(window.functions, "deleteWalletSymbol");
      await fn({ walletId: window.selectedWalletId, symbol: sym });
      window.showToast(`${sym} deleted`, "success");
      await loadSymbolsForWallet(selectedWalletId);
    } catch (e) {
      console.error("deleteWalletSymbol error:", e);
      window.showToast(`Delete failed: ${e.message}`, "error");
    }
  }

  // ===== Auth state =====
  onAuthStateChanged(auth, async (user) => {
    if (user) {
      currentUserId = user.uid;
      if (userEmailSpan) userEmailSpan.textContent = "Loading..."; // Provide immediate feedback
      loginView.classList.remove("active");
      registerView.classList.remove("active");
      appView.classList.add("active");
      if (userEmailSpan) userEmailSpan.textContent = user.email;
      await loadApiKeys();
      ensureWalletPicker();
      await loadWalletsIntoSelect();
    } else {
      currentUserId = null;
      appView.classList.remove("active");
      loginView.classList.add("active");
    }
  });

  // ===== Wallets tab =====
  const createWalletForm = document.getElementById("create-wallet-form");
  const walletsListDiv = document.getElementById("wallets-list");

  if (createWalletForm) {
    createWalletForm.addEventListener("submit", async (e) => {
      e.preventDefault();
      const name = e.target["wallet-name"].value.trim();
      const env = e.target["wallet-env"].value;
      const methodRth = e.target["wallet-method-rth"].value;
      const methodAh = e.target["wallet-method-ah"].value;
      const buyPctRth = parseFloat(e.target["wallet-buy-rth"].value || "0");
      const sellPctRth = parseFloat(e.target["wallet-sell-rth"].value || "0");
      const buyPctAh = parseFloat(e.target["wallet-buy-ah"].value || "0");
      const sellPctAh = parseFloat(e.target["wallet-sell-ah"].value || "0");
      if (!name) { window.showToast("Enter a wallet name", "error"); return; }
      try {
        const fn = window.httpsCallable(window.functions, "createWallet");
        await fn({ name, env, methodRth, methodAh, buyPctRth, sellPctRth, buyPctAh, sellPctAh });
        window.showToast("Wallet created", "success");
        e.target.reset();
        await listWallets();
      } catch (error) {
        console.error("createWallet error:", error);
        window.showToast("Failed to create wallet", "error");
      }
    });
  }

  async function listWallets() {
    if (!walletsListDiv) return;
    walletsListDiv.innerHTML = "<p class='text-gray-400'>Loading...</p>";
    try {
      const fn = window.httpsCallable(window.functions, "listWallets");
      const result = await fn();
      const wallets = result.data.wallets || [];
      if (!wallets.length) {
        walletsListDiv.innerHTML = "<p class='text-gray-400'>No wallets yet.</p>";
        return;
      }
      walletsListDiv.innerHTML = wallets.map(w => {
        const wid = esc(w.wallet_id);
        const wname = esc(w.name);
        const wenv = esc(w.env);
        const enabled = w.enabled ? "ON" : "OFF";
        const toggleClass = w.enabled ? "bg-green-600" : "bg-gray-600";
        return `
          <div class="bg-gray-900 border border-gray-700 rounded p-3 flex items-center justify-between">
            <div>
              <div class="font-semibold text-cyan-400">${wname}</div>
              <div class="text-xs text-gray-400">Env: ${wenv} | ID: ${wid}</div>
            </div>
            <div class="flex items-center gap-2">
              <button class="px-3 py-1 ${toggleClass} hover:opacity-80 rounded text-xs" data-act="toggle" data-wid="${wid}" data-enabled="${w.enabled ? '1' : '0'}">
                ${enabled}
              </button>
              <button class="px-3 py-1 bg-gray-700 hover:bg-gray-600 rounded text-xs" data-act="edit-keys" data-wid="${wid}">
                Edit Keys
              </button>
              <button class="px-3 py-1 bg-red-600 hover:bg-red-700 rounded text-xs" data-act="delete" data-wid="${wid}">
                Delete
              </button>
            </div>
          </div>
        `;
      }).join("");
    } catch (e) {
      console.error("listWallets error:", e);
      walletsListDiv.innerHTML = "<p class='text-red-400'>Failed to load wallets.</p>";
    }
  }

  if (walletsListDiv) {
    walletsListDiv.addEventListener("click", async (e) => {
      const btn = e.target.closest("button");
      if (!btn) return;
      const act = btn.dataset.act;
      const wid = btn.dataset.wid;
      if (act === "toggle") {
        const currentEnabled = btn.dataset.enabled === "1";
        try {
          const fn = window.httpsCallable(window.functions, "updateWallet");
          await fn({ walletId: wid, enabled: !currentEnabled });
          window.showToast("Wallet toggled", "success");
          await listWallets();
        } catch (error) {
          console.error("toggleWallet error:", error);
          window.showToast("Failed to toggle wallet", "error");
        }
      } else if (act === "edit-keys") {
        const polygonKey = prompt("Enter Polygon API key (leave blank to keep current):");
        const alpacaKey = prompt("Enter Alpaca API key (leave blank to keep current):");
        const alpacaSecret = prompt("Enter Alpaca API secret (leave blank to keep current):");
        try {
          const fn = window.httpsCallable(window.functions, "setWalletKeys");
          await fn({ walletId: wid, polygonKey, alpacaKey, alpacaSecret });
          window.showToast("Wallet keys updated", "success");
        } catch (error) {
          console.error("setWalletKeys error:", error);
          window.showToast("Failed to update keys", "error");
        }
      } else if (act === "delete") {
        if (!window.confirm("Delete this wallet? This cannot be undone.")) return;
        try {
          const fn = window.httpsCallable(window.functions, "deleteWallet");
          await fn({ walletId: wid });
          window.showToast("Wallet deleted", "success");
          await listWallets();
        } catch (error) {
          console.error("deleteWallet error:", error);
          window.showToast("Failed to delete wallet", "error");
        }
      }
    });
  }
});
// ============================================================================
// BASELINE BACKFILL - Frontend JavaScript
// ============================================================================

let currentBackfillJobId = null;
let backfillProgressInterval = null;

function initBackfillTab() {
  document.getElementById('preset-last-7')?.addEventListener('click', () => setDatePreset(7));
  document.getElementById('preset-last-30')?.addEventListener('click', () => setDatePreset(30));
  document.getElementById('preset-last-90')?.addEventListener('click', () => setDatePreset(90));
  document.getElementById('preset-last-year')?.addEventListener('click', () => setDatePreset(365));
  
  document.getElementById('select-all-symbols')?.addEventListener('change', (e) => {
    const checkboxes = document.querySelectorAll('#backfill-symbol-checkboxes input[type="checkbox"]');
    checkboxes.forEach(cb => cb.checked = e.target.checked);
  });
  
  document.getElementById('backfill-start-date')?.addEventListener('change', updateBackfillEstimate);
  document.getElementById('backfill-end-date')?.addEventListener('change', updateBackfillEstimate);
  
  document.getElementById('start-backfill')?.addEventListener('click', startBackfill);
  document.getElementById('refresh-progress')?.addEventListener('click', refreshBackfillProgress);
  document.getElementById('cancel-backfill')?.addEventListener('click', cancelBackfill);
  
  if (selectedWalletId) {
    loadBackfillSymbols();
    loadBackfillJobs();
  }
}

function setDatePreset(days) {
  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - days);
  
  document.getElementById('backfill-start-date').value = startDate.toISOString().split('T')[0];
  document.getElementById('backfill-end-date').value = endDate.toISOString().split('T')[0];
  
  updateBackfillEstimate();
}

function updateBackfillEstimate() {
  const startDate = document.getElementById('backfill-start-date')?.value;
  const endDate = document.getElementById('backfill-end-date')?.value;
  const estimateEl = document.getElementById('backfill-estimate');
  
  if (!startDate || !endDate || !estimateEl) return;
  
  const start = new Date(startDate);
  const end = new Date(endDate);
  const days = Math.ceil((end - start) / (1000 * 60 * 60 * 24));
  
  const tradingDays = Math.ceil(days * (252 / 365));
  
  const selectedSymbols = document.querySelectorAll('#backfill-symbol-checkboxes input[type="checkbox"]:checked').length;
  
  const totalCalls = (selectedSymbols + 1) * tradingDays;
  
  const freeHours = Math.ceil(totalCalls / 300);
  const paidMinutes = Math.ceil(totalCalls / 100);
  
  estimateEl.innerHTML = `
    ~${tradingDays} trading days, ${totalCalls} API calls<br>
    Free tier: ~${freeHours} hours | Paid tier: ~${paidMinutes} minutes
  `;
}

async function loadBackfillSymbols() {
  if (!window.selectedWalletId) return;
  
  try {
    const fn = window.httpsCallable(window.functions, "listWalletSymbols");
    const result = await fn({ walletId: window.selectedWalletId });
    const symbols = result.data.symbols || [];
    
    const container = document.getElementById('backfill-symbol-checkboxes');
    if (!container) return;
    
    container.innerHTML = symbols.map(s => `
      <label class="inline-flex items-center">
        <input type="checkbox" value="${s.symbol}" checked class="mr-2" />
        <span>${s.symbol}</span>
      </label>
    `).join('');
    
    updateBackfillEstimate();
    
  } catch (e) {
    console.error("loadBackfillSymbols error:", e);
  }
}

async function startBackfill() {
  if (!window.selectedWalletId) {
    window.showToast("Please select a wallet first", "error");
    return;
  }
  
  const startDate = document.getElementById('backfill-start-date')?.value;
  const endDate = document.getElementById('backfill-end-date')?.value;
  const jobName = document.getElementById('backfill-job-name')?.value;
  const storeMinuteBars = document.getElementById('store-minute-bars')?.checked;
  
  if (!startDate || !endDate) {
    window.showToast("Please select start and end dates", "error");
    return;
  }
  
  const selectedSymbols = Array.from(
    document.querySelectorAll('#backfill-symbol-checkboxes input[type="checkbox"]:checked')
  ).map(cb => cb.value);
  
  if (selectedSymbols.length === 0) {
    window.showToast("Please select at least one symbol", "error");
    return;
  }
  
  const days = Math.ceil((new Date(endDate) - new Date(startDate)) / (1000 * 60 * 60 * 24));
  const confirmed = confirm(
    `Start backfill for ${selectedSymbols.length} symbols over ~${days} days?\n\n` +
    `This will make many API calls to Polygon and may take a while.`
  );
  
  if (!confirmed) return;
  
  const startBtn = document.getElementById('start-backfill');
  if (startBtn) startBtn.disabled = true;
  
  try {
    window.showToast("Starting backfill job...", "info");
    
    const fn = window.httpsCallable(window.functions, "computeBaselinesForDateRange");
    const result = await fn({
      walletId: window.selectedWalletId,
      startDate,
      endDate,
      symbols: selectedSymbols,
      storeMinuteBars,
      jobName: jobName || `Backfill ${startDate} to ${endDate}`
    });
    
    if (result.data.success) {
      currentBackfillJobId = result.data.jobId;
      window.showToast("Backfill job started!", "success");
      
      document.getElementById('backfill-progress-card').style.display = 'block';
      
      startProgressPolling();
      
      loadBackfillJobs();
    }
    
  } catch (e) {
    console.error("startBackfill error:", e);
    window.showToast(`Failed to start backfill: ${e.message}`, "error");
  } finally {
    if (startBtn) startBtn.disabled = false;
  }
}

function startProgressPolling() {
  if (backfillProgressInterval) {
    clearInterval(backfillProgressInterval);
  }
  
  backfillProgressInterval = setInterval(refreshBackfillProgress, 5000);
  
  refreshBackfillProgress();
}

function stopProgressPolling() {
  if (backfillProgressInterval) {
    clearInterval(backfillProgressInterval);
    backfillProgressInterval = null;
  }
}

async function refreshBackfillProgress() {
  if (!currentBackfillJobId) return;
  
  try {
    const fn = window.httpsCallable(window.functions, "getBackfillProgress");
    const result = await fn({ jobId: currentBackfillJobId });
    
    if (result.data.success) {
      updateProgressDisplay(result.data.job, result.data.errors);
      
      if (result.data.job.status === 'completed' || result.data.job.status === 'failed' || result.data.job.status === 'cancelled') {
        stopProgressPolling();
        loadBackfillJobs();
      }
    }
    
  } catch (e) {
    console.error("refreshBackfillProgress error:", e);
  }
}

function updateProgressDisplay(job, errors) {
  const percentage = job.totalDays > 0 ? Math.round((job.completedDays / job.totalDays) * 100) : 0;
  document.getElementById('progress-fill').style.width = `${percentage}%`;
  document.getElementById('progress-percentage').textContent = `${percentage}%`;
  
  document.getElementById('progress-text').textContent = 
    `${job.completedDays} / ${job.totalDays} days completed`;
  
  document.getElementById('progress-status').textContent = job.status;
  document.getElementById('progress-completed').textContent = job.completedDays;
  document.getElementById('progress-failed').textContent = job.failedDays;
  document.getElementById('progress-baselines').textContent = job.totalBaselines;
  
  const statusEl = document.getElementById('progress-status');
  statusEl.className = 'text-lg font-semibold';
  if (job.status === 'completed') {
    statusEl.classList.add('text-green-400');
  } else if (job.status === 'failed' || job.status === 'cancelled') {
    statusEl.classList.add('text-red-400');
  } else {
    statusEl.classList.add('text-yellow-400');
  }
  
  const errorsDiv = document.getElementById('backfill-errors');
  const errorList = document.getElementById('error-list');
  
  if (errors && errors.length > 0) {
    errorsDiv.style.display = 'block';
    errorList.innerHTML = errors.map(err => 
      `<li><strong>${err.trading_day} - ${err.symbol}:</strong> ${err.error_message}</li>`
    ).join('');
  } else {
    errorsDiv.style.display = 'none';
  }
}

async function cancelBackfill() {
  if (!currentBackfillJobId) return;
  
  const confirmed = confirm("Cancel this backfill job?");
  if (!confirmed) return;
  
  try {
    const fn = window.httpsCallable(window.functions, "cancelBackfillJob");
    await fn({ jobId: currentBackfillJobId });
    
    window.showToast("Backfill job cancelled", "info");
    stopProgressPolling();
    loadBackfillJobs();
    
  } catch (e) {
    console.error("cancelBackfill error:", e);
    window.showToast(`Failed to cancel: ${e.message}`, "error");
  }
}

async function loadBackfillJobs() {
  if (!window.selectedWalletId) return;
  
  try {
    const fn = window.httpsCallable(window.functions, "listBackfillJobs");
    const result = await fn({ walletId: window.selectedWalletId, limit: 20 });
    
    const tbody = document.getElementById('backfill-jobs-table');
    if (!tbody) return;
    
    const jobs = result.data.jobs || [];
    
    if (jobs.length === 0) {
      tbody.innerHTML = '<tr><td colspan="8" class="text-center text-gray-400">No backfill jobs yet</td></tr>';
      return;
    }
    
    tbody.innerHTML = jobs.map(job => {
      const statusClass = 
        job.status === 'completed' ? 'text-green-400' :
        job.status === 'failed' ? 'text-red-400' :
        job.status === 'cancelled' ? 'text-gray-400' :
        'text-yellow-400';
      
      const progress = job.totalDays > 0 ? Math.round((job.completedDays / job.totalDays) * 100) : 0;
      
      return `
        <tr>
          <td>${job.jobName}</td>
          <td>${job.startDate} to ${job.endDate}</td>
          <td>${job.symbols.length} symbols</td>
          <td><span class="${statusClass}">${job.status}</span></td>
          <td>
            <div class="text-sm">${job.completedDays}/${job.totalDays} (${progress}%)</div>
            ${job.failedDays > 0 ? `<div class="text-xs text-red-400">${job.failedDays} failed</div>` : ''}
          </td>
          <td>${job.totalBaselines || 0}</td>
          <td>${job.startedAt ? new Date(job.startedAt).toLocaleString() : '-'}</td>
          <td>
            ${job.status === 'running' ? 
              `<button onclick="viewBackfillProgress(${job.id})" class="btn-sm">View</button>` :
              `<button onclick="viewBackfillDetails(${job.id})" class="btn-sm">Details</button>`
            }
          </td>
        </tr>
      `;
    }).join('');
    
  } catch (e) {
    console.error("loadBackfillJobs error:", e);
  }
}

function viewBackfillProgress(jobId) {
  currentBackfillJobId = jobId;
  document.getElementById('backfill-progress-card').style.display = 'block';
  startProgressPolling();
  
  document.getElementById('backfill-progress-card').scrollIntoView({ behavior: 'smooth' });
}

function viewBackfillDetails(jobId) {
  alert(`View details for job ${jobId} - Coming soon!`);
}

// Update showTab function to include backfill tab
// Add this to your existing showTab function:
// case 'backfill':
//   initBackfillTab();
//   break;
// ============================================================================
// DASHBOARD FRONTEND JAVASCRIPT
// Real-time trading data visualization and auto-refresh
// ============================================================================

// Global state
let dashboardRefreshInterval = null;
let lastDashboardData = null;

/**
 * Initialize dashboard tab
 */
   function initDashboard() {
     console.log('[Dashboard] Initializing...');
     
     // Load wallets into dashboard selector
     loadDashboardWallets();
     
     // Set up wallet selector change handler
     const walletSelect = document.getElementById('dashboardWalletSelect');
     if (walletSelect) {
       walletSelect.addEventListener('change', (e) => {
         const walletId = e.target.value;
         if (walletId) {
           console.log('[Dashboard] Wallet changed to:', walletId);
           window.selectedWalletId = walletId;
           localStorage.setItem('selectedDashboardWallet', walletId);
           refreshDashboard();
         }
       });
     }
     
     // Set up auto-refresh toggle
     const autoRefreshToggle = document.getElementById('auto-refresh-toggle');
     const refreshIntervalSelect = document.getElementById('refresh-interval');
     
     if (autoRefreshToggle) {
       autoRefreshToggle.addEventListener('change', (e) => {
         if (e.target.checked) {
           startDashboardAutoRefresh();
         } else {
           stopDashboardAutoRefresh();
         }
       });
     }
     
     if (refreshIntervalSelect) {
       refreshIntervalSelect.addEventListener('change', () => {
         if (autoRefreshToggle && autoRefreshToggle.checked) {
           stopDashboardAutoRefresh();
           startDashboardAutoRefresh();
         }
       });
     }
     
     // Start auto-refresh if enabled
     if (autoRefreshToggle && autoRefreshToggle.checked) {
       startDashboardAutoRefresh();
     }
     
     console.log('[Dashboard] Initialized');
   }
   
   /**
    * Load wallets into dashboard selector
    */
   async function loadDashboardWallets() {
     console.log('[Dashboard] Loading wallets...');
     
     try {
       const user = window.auth.currentUser;
       if (!user) {
         console.error('[Dashboard] No user logged in');
         return;
       }
       
          const listWallets = window.httpsCallable(window.functions, 'listWallets');
       const result = await listWallets();
       
       const walletSelect = document.getElementById('dashboardWalletSelect');
       if (!walletSelect) return;
       
       // Clear existing options except the first one
       walletSelect.innerHTML = '<option value="">Select a wallet...</option>';
       
       if (result.data && result.data.wallets && result.data.wallets.length > 0) {
         result.data.wallets.forEach(wallet => {
           const option = document.createElement('option');
           option.value = wallet.wallet_id;
           option.textContent = wallet.name;
           walletSelect.appendChild(option);
         });
         
         // Restore previously selected wallet or select first wallet
         const savedWalletId = localStorage.getItem('selectedDashboardWallet');
         if (savedWalletId && result.data.wallets.find(w => w.wallet_id === savedWalletId)) {
           walletSelect.value = savedWalletId;
           window.selectedWalletId = savedWalletId;
           refreshDashboard();
         } else if (result.data.wallets.length > 0) {
           const firstWallet = result.data.wallets[0];
           walletSelect.value = firstWallet.wallet_id;
           window.selectedWalletId = firstWallet.wallet_id;
           localStorage.setItem('selectedDashboardWallet', firstWallet.wallet_id);
           refreshDashboard();
         }
         
         console.log('[Dashboard] Loaded', result.data.wallets.length, 'wallets');
       } else {
         console.log('[Dashboard] No wallets found');
       }
     } catch (error) {
       console.error('[Dashboard] Error loading wallets:', error);
       showToast('Failed to load wallets: ' + error.message, 'error');
     }
   }

/**
 * Start auto-refresh timer
 */
function startDashboardAutoRefresh() {
  stopDashboardAutoRefresh(); // Clear any existing interval
  
  const intervalSelect = document.getElementById('refresh-interval');
  const intervalSeconds = parseInt(intervalSelect?.value || 30);
  
  console.log(`[Dashboard] Starting auto-refresh every ${intervalSeconds} seconds`);
  
  dashboardRefreshInterval = setInterval(() => {
    refreshDashboard();
  }, intervalSeconds * 1000);
  
  // Initial refresh
  refreshDashboard();
}

/**
 * Stop auto-refresh timer
 */
function stopDashboardAutoRefresh() {
  if (dashboardRefreshInterval) {
    clearInterval(dashboardRefreshInterval);
    dashboardRefreshInterval = null;
    console.log('[Dashboard] Auto-refresh stopped');
  }
}

/**
 * Refresh dashboard data
 */
async function refreshDashboard() {
  const walletId = window.selectedWalletId;
  
  if (!walletId) {
    console.log('[Dashboard] No wallet selected');
    showDashboardMessage('Please select a wallet to view trading signals');
    return;
  }
  
  console.log('[Dashboard] Refreshing data for wallet:', walletId);
  
  try {
    // Show loading state
    updateLastRefreshTime('Refreshing...');
    
    // Call backend function
    const getDashboardData = window.httpsCallable(window.functions, 'getDashboardData');
    const result = await getDashboardData({ walletId });
    
    if (result.data.success) {
      lastDashboardData = result.data;
      renderDashboard(result.data);
      updateLastRefreshTime('Just now');
      console.log('[Dashboard] Data refreshed successfully');
    } else {
      throw new Error(result.data.message || 'Failed to fetch dashboard data');
    }
    
  } catch (error) {
    console.error('[Dashboard] Error refreshing:', error);
    window.showToast(`Error refreshing dashboard: ${error.message}`, 'error');
    updateLastRefreshTime('Error');
  }
}

/**
 * Render dashboard with data
 */
function renderDashboard(data) {
  const { btcPrice, snapshot, signals, session } = data;
  
  // Update market summary cards
  updateMarketSummary(btcPrice, snapshot, signals);
  
  // Update signals table
  updateSignalsTable(signals, btcPrice, session);
  
  // Update activity log
  loadRecentActivity();
}

/**
 * Update market summary cards
 */
function updateMarketSummary(btcPrice, snapshot, signals) {
  // BTC Price
  const btcPriceEl = document.getElementById('btc-price');
  const btcChangeEl = document.getElementById('btc-change');
  if (btcPriceEl) {
    btcPriceEl.textContent = `$${btcPrice.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  }
  if (btcChangeEl) {
    btcChangeEl.textContent = 'Live'; // Could add 24h change if available
    btcChangeEl.className = 'text-sm text-gray-400';
  }
  
  // Portfolio Value
  const portfolioValueEl = document.getElementById('portfolio-value');
  const portfolioChangeEl = document.getElementById('portfolio-change');
  if (portfolioValueEl && snapshot) {
    portfolioValueEl.textContent = `$${parseFloat(snapshot.equity).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  }
  if (portfolioChangeEl && snapshot) {
    const positionsValue = parseFloat(snapshot.positionsValue || 0);
    const cash = parseFloat(snapshot.cash || 0);
    portfolioChangeEl.textContent = `Positions: $${positionsValue.toLocaleString()} | Cash: $${cash.toLocaleString()}`;
    portfolioChangeEl.className = 'text-sm text-gray-400';
  }
  
  // Available Cash
  const availableCashEl = document.getElementById('available-cash');
  if (availableCashEl && snapshot) {
    availableCashEl.textContent = `$${parseFloat(snapshot.cash).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  }
  
  // Active Signals
  const activeSignalsEl = document.getElementById('active-signals');
  const buySignalsEl = document.getElementById('buy-signals');
  const sellSignalsEl = document.getElementById('sell-signals');
  
  if (signals && signals.length > 0) {
    const buyCount = signals.filter(s => s.signal === 'BUY' || s.signal === 'BOTH').length;
    const sellCount = signals.filter(s => s.signal === 'SELL' || s.signal === 'BOTH').length;
    const totalActive = buyCount + sellCount;
    
    if (activeSignalsEl) activeSignalsEl.textContent = totalActive;
    if (buySignalsEl) buySignalsEl.textContent = `${buyCount} BUY`;
    if (sellSignalsEl) sellSignalsEl.textContent = `${sellCount} SELL`;
  } else {
    if (activeSignalsEl) activeSignalsEl.textContent = '0';
    if (buySignalsEl) buySignalsEl.textContent = '0 BUY';
    if (sellSignalsEl) sellSignalsEl.textContent = '0 SELL';
  }
}

/**
 * Update signals table
 */
function updateSignalsTable(signals, btcPrice, session) {
  const tableBody = document.getElementById('dashboard-table-body');
  const cardsContainer = document.getElementById('dashboard-cards');
  
  if (!signals || signals.length === 0) {
    if (tableBody) {
      tableBody.innerHTML = `
        <tr>
          <td colspan="10" class="px-4 py-8 text-center text-gray-400">
            No trading signals available
          </td>
        </tr>
      `;
    }
    if (cardsContainer) {
      cardsContainer.innerHTML = `
        <div class="p-4 text-center text-gray-400">
          No trading signals available
        </div>
      `;
    }
    return;
  }
  
  // Desktop table view
  if (tableBody) {
    tableBody.innerHTML = signals.map(signal => {
      const signalClass = getSignalClass(signal.signal);
      const signalBadge = getSignalBadge(signal.signal);
      
      return `
        <tr class="hover:bg-gray-750">
          <td class="px-4 py-3 font-medium text-white">${signal.symbol}</td>
          <td class="px-4 py-3 text-gray-300">
            ${signal.baseline ? parseFloat(signal.baseline).toFixed(2) : 'N/A'}
            <div class="text-xs text-gray-500">${signal.method || ''} ${signal.session || ''}</div>
          </td>
          <td class="px-4 py-3 text-gray-300">$${signal.stockPrice ? parseFloat(signal.stockPrice).toFixed(2) : 'N/A'}</td>
          <td class="px-4 py-3 text-gray-300">
            ${signal.currentRatio || 'N/A'}
            <div class="text-xs ${getRatioColor(signal.currentRatio, signal.baseline)}">
              ${getRatioStatus(signal.currentRatio, signal.baseline)}
            </div>
          </td>
          <td class="px-4 py-3 text-green-400">$${signal.buyPrice || 'N/A'}</td>
          <td class="px-4 py-3 text-red-400">$${signal.sellPrice || 'N/A'}</td>
          <td class="px-4 py-3 text-gray-300">${signal.sharesOwned || 0}</td>
          <td class="px-4 py-3 text-gray-300">
            $${signal.availableBudget || '0.00'}
            <div class="text-xs text-gray-500">of $${signal.budget || '0.00'}</div>
          </td>
          <td class="px-4 py-3 text-gray-300">${signal.sharesToBuy || 0}</td>
          <td class="px-4 py-3">${signalBadge}</td>
        </tr>
      `;
    }).join('');
  }
  
  // Mobile card view
  if (cardsContainer) {
    cardsContainer.innerHTML = signals.map(signal => {
      const signalBadge = getSignalBadge(signal.signal);
      
      return `
        <div class="p-4">
          <div class="flex justify-between items-start mb-3">
            <div>
              <div class="text-lg font-bold text-white">${signal.symbol}</div>
              <div class="text-sm text-gray-400">${signal.method || ''} ${signal.session || ''}</div>
            </div>
            ${signalBadge}
          </div>
          <div class="grid grid-cols-2 gap-2 text-sm">
            <div>
              <div class="text-gray-400">Stock Price</div>
              <div class="text-white">$${signal.stockPrice ? parseFloat(signal.stockPrice).toFixed(2) : 'N/A'}</div>
            </div>
            <div>
              <div class="text-gray-400">Baseline</div>
              <div class="text-white">${signal.baseline ? parseFloat(signal.baseline).toFixed(2) : 'N/A'}</div>
            </div>
            <div>
              <div class="text-gray-400">Buy Price</div>
              <div class="text-green-400">$${signal.buyPrice || 'N/A'}</div>
            </div>
            <div>
              <div class="text-gray-400">Sell Price</div>
              <div class="text-red-400">$${signal.sellPrice || 'N/A'}</div>
            </div>
            <div>
              <div class="text-gray-400">Shares Owned</div>
              <div class="text-white">${signal.sharesOwned || 0}</div>
            </div>
            <div>
              <div class="text-gray-400">Shares to Buy</div>
              <div class="text-white">${signal.sharesToBuy || 0}</div>
            </div>
          </div>
        </div>
      `;
    }).join('');
  }
}

/**
 * Get signal CSS class
 */
function getSignalClass(signal) {
  switch (signal) {
    case 'BUY': return 'text-green-400';
    case 'SELL': return 'text-red-400';
    case 'BOTH': return 'text-yellow-400';
    default: return 'text-gray-400';
  }
}

/**
 * Get signal badge HTML
 */
function getSignalBadge(signal) {
  const badges = {
    'BUY': '<span class="px-2 py-1 text-xs font-semibold rounded-full bg-green-900 text-green-300">🟢 BUY</span>',
    'SELL': '<span class="px-2 py-1 text-xs font-semibold rounded-full bg-red-900 text-red-300">🔴 SELL</span>',
    'BOTH': '<span class="px-2 py-1 text-xs font-semibold rounded-full bg-yellow-900 text-yellow-300">🟡 BOTH</span>',
    'HOLD': '<span class="px-2 py-1 text-xs font-semibold rounded-full bg-gray-700 text-gray-400">⚪ HOLD</span>'
  };
  return badges[signal] || badges['HOLD'];
}

/**
 * Get ratio color based on comparison to baseline
 */
function getRatioColor(currentRatio, baseline) {
  if (!currentRatio || !baseline) return 'text-gray-500';
  const ratio = parseFloat(currentRatio);
  if (ratio > baseline) return 'text-green-400';
  if (ratio < baseline) return 'text-red-400';
  return 'text-gray-400';
}

/**
 * Get ratio status text
 */
function getRatioStatus(currentRatio, baseline) {
  if (!currentRatio || !baseline) return '';
  const ratio = parseFloat(currentRatio);
  const diff = ((ratio - baseline) / baseline * 100).toFixed(2);
  if (diff > 0) return `+${diff}% above`;
  if (diff < 0) return `${diff}% below`;
  return 'at baseline';
}

/**
 * Update last refresh time
 */
function updateLastRefreshTime(text) {
  const el = document.getElementById('last-update-time');
  if (el) {
    el.textContent = text;
  }
}

/**
 * Show dashboard message
 */
function showDashboardMessage(message) {
  const tableBody = document.getElementById('dashboard-table-body');
  const cardsContainer = document.getElementById('dashboard-cards');
  
  if (tableBody) {
    tableBody.innerHTML = `
      <tr>
        <td colspan="10" class="px-4 py-8 text-center text-gray-400">
          ${message}
        </td>
      </tr>
    `;
  }
  
  if (cardsContainer) {
    cardsContainer.innerHTML = `
      <div class="p-4 text-center text-gray-400">
        ${message}
      </div>
    `;
  }
}

/**
 * Load recent activity
 */
async function loadRecentActivity() {
  const walletId = window.selectedWalletId;
  if (!walletId) return;
  
  try {
    const getRecentActivity = window.httpsCallable(window.functions, 'getRecentActivity');
    const result = await getRecentActivity({ walletId, limit: 20 });
    
    if (result.data.success) {
      renderActivityLog(result.data.activity);
    }
  } catch (error) {
    console.error('[Dashboard] Error loading activity:', error);
  }
}

/**
 * Render activity log
 */
function renderActivityLog(activity) {
  const tbody = document.getElementById('activity-log-body');
  if (!tbody) return;
  
  if (!activity || activity.length === 0) {
    tbody.innerHTML = `
      <tr>
        <td colspan="7" class="px-4 py-8 text-center text-gray-400">
          No recent activity
        </td>
      </tr>
    `;
    return;
  }
  
  tbody.innerHTML = activity.map(item => {
    const time = new Date(item.created_at).toLocaleString();
    const actionClass = item.side === 'buy' ? 'text-green-400' : 'text-red-400';
    const statusClass = item.status === 'filled' ? 'text-green-400' : 
                       item.status === 'cancelled' ? 'text-gray-400' : 'text-yellow-400';
    
    return `
      <tr class="hover:bg-gray-750">
        <td class="px-4 py-3 text-gray-300 text-xs">${time}</td>
        <td class="px-4 py-3 font-medium text-white">${item.symbol}</td>
        <td class="px-4 py-3 ${actionClass} uppercase">${item.side}</td>
        <td class="px-4 py-3 text-gray-300">${item.qty}</td>
        <td class="px-4 py-3 text-gray-300">$${parseFloat(item.limit_price).toFixed(2)}</td>
        <td class="px-4 py-3 ${statusClass}">${item.status}</td>
        <td class="px-4 py-3 text-gray-400 text-xs">${item.order_id || 'N/A'}</td>
      </tr>
    `;
  }).join('');
}

/**
 * Execute trades (manual execution)
 */
async function executeTrades() {
  const walletId = window.selectedWalletId;
  
  if (!walletId) {
    window.showToast('Please select a wallet first', 'error');
    return;
  }
  
  if (!confirm('Execute trades for this wallet? This will place real orders in your paper trading account.')) {
    return;
  }
  
  try {
    window.showToast('Executing trades...', 'info');
    
    const runWalletExecute = window.httpsCallable(window.functions, 'runWalletExecute');
    const result = await runWalletExecute({ walletId });
    
    if (result.data.success) {
      window.showToast('Trades executed successfully!', 'success');
      // Refresh dashboard to show new activity
      setTimeout(() => refreshDashboard(), 2000);
    } else {
      throw new Error(result.data.message || 'Execution failed');
    }
    
  } catch (error) {
    console.error('[Dashboard] Execution error:', error);
    window.showToast(`Execution error: ${error.message}`, 'error');
  }
}

  // Make dashboard functions globally accessible
  window.refreshDashboard = refreshDashboard;
  window.executeTrades = executeTrades;

// Initialize dashboard when DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initDashboard);
} else {
  initDashboard();
}
// ============================================================================
// SETTINGS TAB
// ============================================================================

function initSettings() {
  console.log('[Settings] Initializing settings tab');
  loadSystemSettings();
  loadWalletManagement();
}

async function loadSystemSettings() {
  try {
    const getSystemSettings = window.httpsCallable(window.functions, 'getSystemSettings');
    const result = await getSystemSettings();
    
    if (result.data && result.data.settings) {
      const settings = result.data.settings;
      
      // Update UI with current settings
      const systemEnabledToggle = document.getElementById('systemEnabledToggle');
      if (systemEnabledToggle) {
        systemEnabledToggle.checked = settings.system_enabled === 'true';
        updateSystemStatusText(settings.system_enabled === 'true');
      }
      
      const executionFrequency = document.getElementById('executionFrequency');
      if (executionFrequency) {
        executionFrequency.value = settings.execution_frequency || '60';
      }
      
      const dryRunToggle = document.getElementById('dryRunToggle');
      if (dryRunToggle) {
        dryRunToggle.checked = settings.dry_run_mode === 'true';
      }
      
        
        // Load global trading hours
        const globalTradingStart = document.getElementById('globalTradingStart');
        if (globalTradingStart && settings.global_trading_start_et) {
          globalTradingStart.value = settings.global_trading_start_et;
        }
        
        const globalTradingEnd = document.getElementById('globalTradingEnd');
        if (globalTradingEnd && settings.global_trading_end_et) {
          globalTradingEnd.value = settings.global_trading_end_et;
        }
        
      console.log('[Settings] Loaded system settings:', settings);
    }
  } catch (error) {
    console.error('[Settings] Error loading system settings:', error);
    window.showToast('Failed to load system settings: ' + error.message, 'error');
  }
}

function updateSystemStatusText(enabled) {
  const statusText = document.getElementById('systemStatusText');
  const statusBadge = document.getElementById('systemStatusBadge');
  
  if (statusText) {
    statusText.textContent = enabled ? 'ON' : 'OFF';
    statusText.className = enabled ? 'status-on' : 'status-off';
  }
  
  if (statusBadge) {
    statusBadge.textContent = enabled ? '🟢 SYSTEM ON' : '🔴 SYSTEM OFF';
    statusBadge.className = enabled ? 'badge badge-success' : 'badge badge-danger';
  }
}

async function toggleSystemEnabled() {
  const toggle = document.getElementById('systemEnabledToggle');
  const enabled = toggle.checked;
  
  try {
    const updateSystemSettings = window.httpsCallable(window.functions, 'updateSystemSettings');
    await updateSystemSettings({
      settingKey: 'system_enabled',
      settingValue: enabled ? 'true' : 'false'
    });
    
    updateSystemStatusText(enabled);
    window.showToast(`System ${enabled ? 'enabled' : 'disabled'} successfully`, 'success');
    console.log(`[Settings] System ${enabled ? 'enabled' : 'disabled'}`);
  } catch (error) {
    console.error('[Settings] Error toggling system:', error);
    toggle.checked = !enabled; // Revert toggle
    window.showToast('Failed to update system status: ' + error.message, 'error');
  }
}

async function updateExecutionFrequency() {
  const select = document.getElementById('executionFrequency');
  const frequency = select.value;
  
  try {
    const updateSystemSettings = window.httpsCallable(window.functions, 'updateSystemSettings');
    await updateSystemSettings({
      settingKey: 'execution_frequency',
      settingValue: frequency
    });
    
    window.showToast('Execution frequency updated', 'success');
    console.log(`[Settings] Execution frequency set to ${frequency} seconds`);
  } catch (error) {
    console.error('[Settings] Error updating frequency:', error);
    window.showToast('Failed to update frequency: ' + error.message, 'error');
  }
}

async function toggleDryRun() {
  const toggle = document.getElementById('dryRunToggle');
  const enabled = toggle.checked;
  
  try {
    const updateSystemSettings = window.httpsCallable(window.functions, 'updateSystemSettings');
    await updateSystemSettings({
      settingKey: 'dry_run_mode',
      settingValue: enabled ? 'true' : 'false'
    });
    
    window.showToast(`Dry run mode ${enabled ? 'enabled' : 'disabled'}`, 'success');
    console.log(`[Settings] Dry run mode ${enabled ? 'enabled' : 'disabled'}`);
  } catch (error) {
    console.error('[Settings] Error toggling dry run:', error);
    toggle.checked = !enabled; // Revert toggle
    window.showToast('Failed to update dry run mode: ' + error.message, 'error');
  }
}

  async function updateGlobalTradingHours() {
    const startInput = document.getElementById('globalTradingStart');
    const endInput = document.getElementById('globalTradingEnd');
    
    if (!startInput || !endInput) return;
    
    const startTime = startInput.value;
    const endTime = endInput.value;
    
    if (!startTime || !endTime) {
      window.showToast('Please enter both start and end times', 'error');
      return;
    }
    
    try {
      const updateSystemSettings = window.httpsCallable(window.functions, 'updateSystemSettings');
      
      // Update start time
      await updateSystemSettings({
        settingKey: 'global_trading_start_et',
        settingValue: startTime
      });
      
      // Update end time
      await updateSystemSettings({
        settingKey: 'global_trading_end_et',
        settingValue: endTime
      });
      
      window.showToast(`Global trading hours updated: ${startTime} - ${endTime} ET`, 'success');
      console.log(`[Settings] Global trading hours updated: ${startTime} - ${endTime} ET`);
    } catch (error) {
      console.error('[Settings] Error updating trading hours:', error);
      window.showToast('Failed to update trading hours: ' + error.message, 'error');
    }
  }
  
async function loadWalletManagement() {
  try {
    const listWallets = window.httpsCallable(window.functions, 'listWallets');
    const result = await listWallets();
    
    if (result.data && result.data.wallets) {
      const tbody = document.getElementById('walletManagementBody');
      if (!tbody) return;
      
      tbody.innerHTML = '';
      
      for (const wallet of result.data.wallets) {
        // Get wallet status
        const getWalletStatus = window.httpsCallable(window.functions, 'getWalletStatus');
        const statusResult = await getWalletStatus({ walletId: wallet.wallet_id });
        const status = statusResult.data;
        
        const row = document.createElement('tr');
        row.innerHTML = `
          <td>${esc(wallet.name)}</td>
          <td>
            <span class="status-badge ${status.enabled ? 'status-enabled' : 'status-disabled'}">
              ${status.enabled ? '🟢 Enabled' : '⚫ Disabled'}
            </span>
          </td>
          <td>${status.lastExecution ? new Date(status.lastExecution).toLocaleString() : 'Never'}</td>
          <td>${status.orderCount || 0}</td>
          <td>
            <label class="switch">
              <input type="checkbox" ${status.enabled ? 'checked' : ''} 
                     onchange="toggleWalletEnabled('${wallet.wallet_id}', this.checked)">
              <span class="slider"></span>
            </label>
          </td>
        `;
        tbody.appendChild(row);
      }
      
      console.log('[Settings] Loaded wallet management for', result.data.wallets.length, 'wallets');
    }
  } catch (error) {
    console.error('[Settings] Error loading wallet management:', error);
    window.showToast('Failed to load wallet management: ' + error.message, 'error');
  }
}

async function toggleWalletEnabled(walletId, enabled) {
  try {
    const updateWalletEnabled = window.httpsCallable(window.functions, 'updateWalletEnabled');
    await updateWalletEnabled({ walletId, enabled });
    
    window.showToast(`Wallet ${enabled ? 'enabled' : 'disabled'} successfully`, 'success');
    console.log(`[Settings] Wallet ${walletId} ${enabled ? 'enabled' : 'disabled'}`);
    
    // Reload wallet management to update display
    loadWalletManagement();
  } catch (error) {
    console.error('[Settings] Error toggling wallet:', error);
    window.showToast('Failed to update wallet status: ' + error.message, 'error');
    // Reload to revert the toggle
    loadWalletManagement();
  }
}

// Make settings functions globally accessible
window.initSettings = initSettings;
window.toggleSystemEnabled = toggleSystemEnabled;
window.updateExecutionFrequency = updateExecutionFrequency;
window.toggleDryRun = toggleDryRun;
window.toggleWalletEnabled = toggleWalletEnabled;


// ============================================================================
// KPI DASHBOARD FUNCTIONS
// ============================================================================

// ============================================================================
// KPI TAB JAVASCRIPT
// ============================================================================

// KPI Data Cache
let kpiWalletId = null;
let kpiChart = null;

// Initialize KPI Tab
function initKPITab() {
  const kpiWalletSelect = document.getElementById('kpi-wallet-select');
  const refreshBtn = document.getElementById('refresh-kpi');
  const updateChartBtn = document.getElementById('kpi-update-chart');
  const dateRangeSelect = document.getElementById('kpi-date-range');

  if (kpiWalletSelect) {
    kpiWalletSelect.addEventListener('change', (e) => {
      kpiWalletId = e.target.value;
      if (kpiWalletId) {
        loadKPIData();
      }
    });
  }

  if (refreshBtn) {
    refreshBtn.addEventListener('click', () => {
      if (kpiWalletId) {
        loadKPIData();
      } else {
        window.showToast('Please select a wallet first', 'warning');
      }
    });
  }

  if (updateChartBtn) {
    updateChartBtn.addEventListener('click', () => {
      if (kpiWalletId) {
        const days = dateRangeSelect.value;
        loadPerformanceData(days);
      }
    });
  }

  // Populate wallet dropdown
  populateKPIWalletDropdown();
}

// Populate wallet dropdown
async function populateKPIWalletDropdown() {
  const select = document.getElementById('kpi-wallet-select');
  if (!select) return;

  try {
    const fn = window.httpsCallable(window.functions, 'listWallets');
    const result = await fn({});
    const wallets = result.data.wallets || [];

    select.innerHTML = '<option value="">Select Wallet...</option>';
    wallets.forEach(w => {
      const option = document.createElement('option');
      option.value = w.wallet_id;
      option.textContent = `${w.name} (${w.env})`;
      select.appendChild(option);
    });
  } catch (e) {
      
      // Auto-select first wallet if available
      if (wallets.length > 0) {
        select.value = wallets[0].wallet_id;
        kpiWalletId = wallets[0].wallet_id;
        // Trigger data load
        loadKPIData();
      }
    console.error('Failed to load wallets:', e);
  }
}

// Load all KPI data
async function loadKPIData() {
  if (!kpiWalletId) return;

  window.showToast('Loading KPI data...', 'info');

  try {
    await Promise.all([
      loadPortfolioSummary(),
      loadCurrentPositions(),
      loadRecentOrders(),
      loadPerformanceData(30),
      loadTradingActivity()
    ]);

    window.showToast('KPI data loaded successfully', 'success');
  } catch (e) {
    console.error('Failed to load KPI data:', e);
    window.showToast('Failed to load KPI data', 'error');
  }
}

// Load portfolio summary
async function loadPortfolioSummary() {
  try {
    const fn = window.httpsCallable(window.functions, 'getAccountInfo');
    const result = await fn({ walletId: kpiWalletId });
    const account = result.data.account || {};

    document.getElementById('kpi-total-equity').textContent = formatCurrency(account.equity || 0);
    document.getElementById('kpi-cash').textContent = formatCurrency(account.cash || 0);
    document.getElementById('kpi-buying-power').textContent = `Buying Power: ${formatCurrency(account.buying_power || 0)}`;
    
    const totalPL = (account.equity || 0) - (account.last_equity || account.equity || 0);
    const plPercent = account.last_equity ? ((totalPL / account.last_equity) * 100).toFixed(2) : '0.00';
    
    document.getElementById('kpi-total-pl').textContent = formatCurrency(totalPL);
    document.getElementById('kpi-pl-percent').textContent = `${plPercent}%`;
    document.getElementById('kpi-pl-percent').className = totalPL >= 0 ? 'text-green-200 text-xs mt-2' : 'text-red-200 text-xs mt-2';

    const equityChange = (account.equity || 0) - (account.last_equity || account.equity || 0);
    const equityChangePercent = account.last_equity ? ((equityChange / account.last_equity) * 100).toFixed(2) : '0.00';
    document.getElementById('kpi-equity-change').textContent = `${equityChange >= 0 ? '+' : ''}${formatCurrency(equityChange)} (${equityChangePercent}%)`;

  } catch (e) {
    console.error('Failed to load portfolio summary:', e);
  }
}

// Load current positions
async function loadCurrentPositions() {
  try {
    const fn = window.httpsCallable(window.functions, 'getPositions');
    const result = await fn({ walletId: kpiWalletId });
    const positions = result.data.positions || [];

    const tbody = document.getElementById('kpi-positions-body');
    if (!positions.length) {
      tbody.innerHTML = '<tr><td class="table-cell" colspan="8">No open positions</td></tr>';
      document.getElementById('kpi-position-count').textContent = '0';
      document.getElementById('kpi-total-value').textContent = 'Value: $0.00';
      return;
    }

    const totalValue = positions.reduce((sum, p) => sum + parseFloat(p.market_value || 0), 0);
    document.getElementById('kpi-position-count').textContent = positions.length;
    document.getElementById('kpi-total-value').textContent = `Value: ${formatCurrency(totalValue)}`;

    tbody.innerHTML = positions.map(p => {
      const pl = parseFloat(p.unrealized_pl || 0);
      const plPercent = parseFloat(p.unrealized_plpc || 0) * 100;
      const changeToday = parseFloat(p.change_today || 0);
      
      return `
        <tr>
          <td class="table-cell font-semibold">${esc(p.symbol)}</td>
          <td class="table-cell">${p.qty}</td>
          <td class="table-cell">${formatCurrency(p.current_price)}</td>
          <td class="table-cell">${formatCurrency(p.market_value)}</td>
          <td class="table-cell">${formatCurrency(p.cost_basis)}</td>
          <td class="table-cell ${pl >= 0 ? 'text-green-400' : 'text-red-400'}">${formatCurrency(pl)}</td>
          <td class="table-cell ${plPercent >= 0 ? 'text-green-400' : 'text-red-400'}">${plPercent.toFixed(2)}%</td>
          <td class="table-cell ${changeToday >= 0 ? 'text-green-400' : 'text-red-400'}">${changeToday.toFixed(2)}%</td>
        </tr>
      `;
    }).join('');

  } catch (e) {
    console.error('Failed to load positions:', e);
    document.getElementById('kpi-positions-body').innerHTML = '<tr><td class="table-cell" colspan="8">Failed to load positions</td></tr>';
  }
}

// Load recent orders
async function loadRecentOrders() {
  try {
    const fn = window.httpsCallable(window.functions, 'getRecentOrders');
    const result = await fn({ walletId: kpiWalletId, limit: 10 });
    const orders = result.data.orders || [];

    const tbody = document.getElementById('kpi-orders-body');
    if (!orders.length) {
      tbody.innerHTML = '<tr><td class="table-cell" colspan="7">No recent orders</td></tr>';
      return;
    }

    tbody.innerHTML = orders.map(o => {
      const statusClass = {
        'filled': 'text-green-400',
        'accepted': 'text-yellow-400',
        'pending_new': 'text-blue-400',
        'canceled': 'text-gray-400',
        'rejected': 'text-red-400'
      }[o.status] || 'text-gray-400';

      const sideClass = o.side === 'buy' ? 'text-cyan-400' : 'text-orange-400';

      return `
        <tr>
          <td class="table-cell text-xs">${formatDateTime(o.created_at)}</td>
          <td class="table-cell font-semibold">${esc(o.symbol)}</td>
          <td class="table-cell ${sideClass}">${o.side.toUpperCase()}</td>
          <td class="table-cell">${o.qty}</td>
          <td class="table-cell">${formatCurrency(o.limit_price)}</td>
          <td class="table-cell ${statusClass}">${o.status}</td>
          <td class="table-cell text-xs">${o.alpaca_order_id ? o.alpaca_order_id.substring(0, 8) + '...' : 'N/A'}</td>
        </tr>
      `;
    }).join('');

  } catch (e) {
    console.error('Failed to load recent orders:', e);
    document.getElementById('kpi-orders-body').innerHTML = '<tr><td class="table-cell" colspan="7">Failed to load orders</td></tr>';
  }
}

// Load performance data
async function loadPerformanceData(days) {
  try {
    const fn = window.httpsCallable(window.functions, 'getPerformanceData');
    const result = await fn({ walletId: kpiWalletId, days: days === 'all' ? 365 : parseInt(days) });
    const performance = result.data.performance || [];

    // Update performance table
    const tbody = document.getElementById('kpi-performance-body');
    if (!performance.length) {
      tbody.innerHTML = '<tr><td class="table-cell" colspan="7">No performance data available</td></tr>';
      return;
    }

    tbody.innerHTML = performance.map(p => {
      const roi = parseFloat(p.roi_percent || 0);
      const winRate = parseFloat(p.win_rate || 0);

      return `
        <tr>
          <td class="table-cell font-semibold">${esc(p.symbol)}</td>
          <td class="table-cell">${p.total_trades || 0}</td>
          <td class="table-cell text-cyan-400">${p.buy_signals || 0}</td>
          <td class="table-cell text-orange-400">${p.sell_signals || 0}</td>
          <td class="table-cell">${formatCurrency(p.avg_price || 0)}</td>
          <td class="table-cell ${roi >= 0 ? 'text-green-400' : 'text-red-400'}">${roi.toFixed(2)}%</td>
          <td class="table-cell">${winRate.toFixed(1)}%</td>
        </tr>
      `;
    }).join('');

    // Update chart
    updatePerformanceChart(performance);

  } catch (e) {
    console.error('Failed to load performance data:', e);
    document.getElementById('kpi-performance-body').innerHTML = '<tr><td class="table-cell" colspan="7">Failed to load performance data</td></tr>';
  }
}

// Update performance chart
function updatePerformanceChart(performance) {
  const ctx = document.getElementById('kpi-performance-chart');
  if (!ctx) return;

  const labels = performance.map(p => p.symbol);
  const roiData = performance.map(p => parseFloat(p.roi_percent || 0));
  const colors = roiData.map(roi => roi >= 0 ? 'rgba(34, 197, 94, 0.8)' : 'rgba(239, 68, 68, 0.8)');

  if (kpiChart) {
    kpiChart.destroy();
  }

  kpiChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: labels,
      datasets: [{
        label: 'ROI %',
        data: roiData,
        backgroundColor: colors,
        borderColor: colors.map(c => c.replace('0.8', '1')),
        borderWidth: 1
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      plugins: {
        legend: {
          display: false
        },
        title: {
          display: true,
          text: 'Return on Investment by Symbol',
          color: '#fff',
          font: { size: 16 }
        }
      },
      scales: {
        y: {
          beginAtZero: true,
          grid: { color: 'rgba(255, 255, 255, 0.1)' },
          ticks: { 
            color: '#9ca3af',
            callback: function(value) {
              return value + '%';
            }
          }
        },
        x: {
          grid: { color: 'rgba(255, 255, 255, 0.1)' },
          ticks: { color: '#9ca3af' }
        }
      }
    }
  });
}

// Load trading activity
async function loadTradingActivity() {
  try {
    const fn = window.httpsCallable(window.functions, 'getTradingActivity');
    const result = await fn({ walletId: kpiWalletId, days: 7 });
    const activity = result.data.activity || [];

    const hoursContainer = document.getElementById('kpi-activity-hours');
    const barsContainer = document.getElementById('kpi-activity-bars');

    if (!activity.length) {
      hoursContainer.innerHTML = '<div class="text-gray-400 text-xs">No activity</div>';
      barsContainer.innerHTML = '';
      return;
    }

    // Create 24-hour grid
    const hourCounts = new Array(24).fill(0);
    activity.forEach(a => {
      const hour = parseInt(a.hour_et);
      hourCounts[hour] = parseInt(a.order_count);
    });

    const maxCount = Math.max(...hourCounts);

    hoursContainer.innerHTML = hourCounts.map((_, i) => 
      `<div class="text-xs text-gray-400 text-center">${i}</div>`
    ).join('');

    barsContainer.innerHTML = hourCounts.map(count => {
      const height = maxCount > 0 ? (count / maxCount) * 100 : 0;
      const color = count > 0 ? 'bg-cyan-500' : 'bg-gray-700';
      return `<div class="relative h-12 flex items-end">
        <div class="${color} w-full rounded-t" style="height: ${height}%" title="${count} orders"></div>
      </div>`;
    }).join('');

  } catch (e) {
    console.error('Failed to load trading activity:', e);
  }
}

// Helper functions
function formatCurrency(value) {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  }).format(value || 0);
}

function formatDateTime(dateStr) {
  if (!dateStr) return 'N/A';
  const date = new Date(dateStr);
  return date.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit'
  });
}

function esc(str) {
  const div = document.createElement('div');
  div.textContent = str;
  return div.innerHTML;
}

// Initialize when DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initKPITab);
} else {
  initKPITab();
}

// Global variables
let portfolioChart = null;
let activityChart = null;

// ============================================================================
// 1. LOAD AND DISPLAY PORTFOLIO HISTORY (Equity Curve)
// ============================================================================
async function loadPortfolioHistory(period = '1M') {
  try {
    const fn = window.httpsCallable(window.functions, 'getPortfolioHistory');
    const result = await fn({ 
      walletId: kpiWalletId, 
      period: period,
      timeframe: period === '1D' ? '5Min' : '1D'
    });
    
    const data = result.data;
    
    // Update equity curve chart
    updateEquityCurveChart(data);
    
    // Update summary stats
    updatePortfolioStats(data);
    
  } catch (e) {
    console.error('Failed to load portfolio history:', e);
  }
}

function updateEquityCurveChart(data) {
  const canvas = document.getElementById('equity-curve-chart');
  if (!canvas) return;

  // Destroy existing chart
  if (portfolioChart) {
    portfolioChart.destroy();
  }

  // Format timestamps
  const labels = data.timestamps.map(ts => {
    const date = new Date(ts * 1000);
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
  });

  // Create gradient
  const ctx = canvas.getContext('2d');
  const gradient = ctx.createLinearGradient(0, 0, 0, 400);
  gradient.addColorStop(0, 'rgba(6, 182, 212, 0.4)');
  gradient.addColorStop(1, 'rgba(6, 182, 212, 0.0)');

  portfolioChart = new Chart(canvas, {
    type: 'line',
    data: {
      labels: labels,
      datasets: [{
        label: 'Portfolio Equity',
        data: data.equity,
        borderColor: 'rgb(6, 182, 212)',
        backgroundColor: gradient,
        borderWidth: 2,
        fill: true,
        tension: 0.4,
        pointRadius: 0,
        pointHoverRadius: 5
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          display: false
        },
        title: {
          display: true,
          text: 'Portfolio Equity Over Time',
          color: '#fff',
          font: {
            size: 16,
            weight: 'bold'
          }
        },
        tooltip: {
          mode: 'index',
          intersect: false,
          callbacks: {
            label: function(context) {
              const equity = context.parsed.y;
              const index = context.dataIndex;
              const pl = data.profit_loss[index];
              const plPct = data.profit_loss_pct[index];
              return [
                `Equity: $${equity.toFixed(2)}`,
                `P&L: ${pl >= 0 ? '+' : ''}$${pl.toFixed(2)}`,
                `P&L %: ${plPct >= 0 ? '+' : ''}${plPct.toFixed(2)}%`
              ];
            }
          }
        }
      },
      scales: {
        y: {
          beginAtZero: false,
          grid: {
            color: 'rgba(75, 85, 99, 0.3)'
          },
          ticks: {
            color: '#9CA3AF',
            callback: function(value) {
              return '$' + value.toLocaleString();
            }
          }
        },
        x: {
          grid: {
            display: false
          },
          ticks: {
            color: '#9CA3AF',
            maxRotation: 45,
            minRotation: 45
          }
        }
      }
    }
  });
}

function updatePortfolioStats(data) {
  const equity = data.equity;
  const profitLoss = data.profit_loss;
  const profitLossPct = data.profit_loss_pct;
  
  if (equity.length === 0) return;
  
  const currentEquity = equity[equity.length - 1];
  const currentPL = profitLoss[profitLoss.length - 1];
  const currentPLPct = profitLossPct[profitLossPct.length - 1];
  const baseValue = data.base_value;
  
  // Calculate max drawdown
  let maxEquity = equity[0];
  let maxDrawdown = 0;
  
  equity.forEach(e => {
    if (e > maxEquity) maxEquity = e;
    const drawdown = ((e - maxEquity) / maxEquity) * 100;
    if (drawdown < maxDrawdown) maxDrawdown = drawdown;
  });
  
  // Update stats display
  document.getElementById('portfolio-current-equity').textContent = `$${currentEquity.toFixed(2)}`;
  document.getElementById('portfolio-base-value').textContent = `$${baseValue.toFixed(2)}`;
  document.getElementById('portfolio-total-pl').textContent = `${currentPL >= 0 ? '+' : ''}$${currentPL.toFixed(2)}`;
  document.getElementById('portfolio-total-pl-pct').textContent = `${currentPLPct >= 0 ? '+' : ''}${currentPLPct.toFixed(2)}%`;
  document.getElementById('portfolio-max-drawdown').textContent = `${maxDrawdown.toFixed(2)}%`;
  
  // Color code P&L
  const plClass = currentPL >= 0 ? 'text-green-400' : 'text-red-400';
  document.getElementById('portfolio-total-pl').className = plClass + ' text-2xl font-bold';
  document.getElementById('portfolio-total-pl-pct').className = plClass + ' text-lg';
}

// ============================================================================
// 2. LOAD AND DISPLAY ACCOUNT ACTIVITIES
// ============================================================================
async function loadAccountActivities(activityTypes = null) {
  try {
    const fn = window.httpsCallable(window.functions, 'getAccountActivities');
    const result = await fn({ 
      walletId: kpiWalletId,
      activityTypes: activityTypes,
      pageSize: 50
    });
    
    const activities = result.data.activities;
    
    // Update activities table
    updateActivitiesTable(activities);
    
    // Update activity summary
    updateActivitySummary(activities);
    
  } catch (e) {
    console.error('Failed to load account activities:', e);
  }
}

function updateActivitiesTable(activities) {
  const tbody = document.getElementById('activities-table-body');
  if (!tbody) return;
  
  if (!activities || activities.length === 0) {
    tbody.innerHTML = '<tr><td colspan="6" class="table-cell text-center text-gray-400">No activities found</td></tr>';
    return;
  }
  
  tbody.innerHTML = activities.map(activity => {
    const date = new Date(activity.date);
    const dateStr = date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
    
    const typeClass = activity.activity_type === 'FILL' ? 'text-cyan-400' :
                      activity.activity_type === 'DIV' ? 'text-green-400' :
                      activity.activity_type.startsWith('DIV') ? 'text-green-300' :
                      'text-gray-400';
    
    const amountClass = activity.net_amount >= 0 ? 'text-green-400' : 'text-red-400';
    
    const sideClass = activity.side === 'buy' ? 'text-cyan-400' : 
                      activity.side === 'sell' ? 'text-orange-400' : 
                      'text-gray-400';
    
    return `
      <tr class="hover:bg-gray-700">
        <td class="table-cell text-sm">${dateStr}</td>
        <td class="table-cell ${typeClass} font-semibold">${activity.activity_type}</td>
        <td class="table-cell">${activity.symbol || '-'}</td>
        <td class="table-cell ${sideClass}">${activity.side ? activity.side.toUpperCase() : '-'}</td>
        <td class="table-cell text-right">${activity.qty > 0 ? activity.qty.toFixed(4) : '-'}</td>
        <td class="table-cell text-right">${activity.price > 0 ? '$' + activity.price.toFixed(2) : '-'}</td>
        <td class="table-cell text-right ${amountClass} font-semibold">
          ${activity.net_amount >= 0 ? '+' : ''}$${activity.net_amount.toFixed(2)}
        </td>
      </tr>
    `;
  }).join('');
}

function updateActivitySummary(activities) {
  const fills = activities.filter(a => a.activity_type === 'FILL');
  const dividends = activities.filter(a => a.activity_type.startsWith('DIV'));
  const fees = activities.filter(a => a.activity_type.includes('FEE'));
  
  const totalFills = fills.length;
  const totalDividends = dividends.reduce((sum, d) => sum + d.net_amount, 0);
  const totalFees = fees.reduce((sum, f) => sum + Math.abs(f.net_amount), 0);
  
  document.getElementById('activity-total-fills').textContent = totalFills;
  document.getElementById('activity-total-dividends').textContent = `$${totalDividends.toFixed(2)}`;
  document.getElementById('activity-total-fees').textContent = `$${totalFees.toFixed(2)}`;
}

// ============================================================================
// 3. LOAD AND DISPLAY CLOSED POSITIONS
// ============================================================================
async function loadClosedPositions() {
  try {
    const fn = window.httpsCallable(window.functions, 'getClosedPositions');
    const result = await fn({ walletId: kpiWalletId, days: 90 });
    
    const closedPositions = result.data.closedPositions;
    
    // Update closed positions table
    updateClosedPositionsTable(closedPositions);
    
  } catch (e) {
    console.error('Failed to load closed positions:', e);
  }
}

function updateClosedPositionsTable(positions) {
  const tbody = document.getElementById('closed-positions-body');
  if (!tbody) return;
  
  if (!positions || positions.length === 0) {
    tbody.innerHTML = '<tr><td colspan="7" class="table-cell text-center text-gray-400">No closed positions</td></tr>';
    return;
  }
  
  tbody.innerHTML = positions.map(pos => {
    const roiClass = pos.roi_percent > 0 ? 'text-green-400' : pos.roi_percent < 0 ? 'text-red-400' : 'text-gray-400';
    const plClass = pos.realized_pl > 0 ? 'text-green-400' : pos.realized_pl < 0 ? 'text-red-400' : 'text-gray-400';
    
    return `
      <tr class="hover:bg-gray-700">
        <td class="table-cell font-semibold">${esc(pos.symbol)}</td>
        <td class="table-cell text-right">${pos.total_buys}</td>
        <td class="table-cell text-right">${pos.total_sells}</td>
        <td class="table-cell text-right">$${pos.total_invested.toFixed(2)}</td>
        <td class="table-cell text-right">$${pos.total_returned.toFixed(2)}</td>
        <td class="table-cell text-right ${plClass} font-bold">
          ${pos.realized_pl >= 0 ? '+' : ''}$${pos.realized_pl.toFixed(2)}
        </td>
        <td class="table-cell text-right ${roiClass} font-bold">
          ${pos.roi_percent >= 0 ? '+' : ''}${pos.roi_percent.toFixed(2)}%
        </td>
      </tr>
    `;
  }).join('');
}

// ============================================================================
// 4. LOAD AND DISPLAY ORDER EXECUTION METRICS
// ============================================================================
async function loadOrderMetrics() {
  try {
    const fn = window.httpsCallable(window.functions, 'getOrderMetrics');
    const result = await fn({ walletId: kpiWalletId, days: 30 });
    
    const { metrics, totals } = result.data;
    
    // Update metrics table
    updateOrderMetricsTable(metrics, totals);
    
    // Update metrics summary
    updateMetricsSummary(totals);
    
  } catch (e) {
    console.error('Failed to load order metrics:', e);
  }
}

function updateOrderMetricsTable(metrics, totals) {
  const tbody = document.getElementById('order-metrics-body');
  if (!tbody) return;
  
  if (!metrics || metrics.length === 0) {
    tbody.innerHTML = '<tr><td colspan="7" class="table-cell text-center text-gray-400">No order data</td></tr>';
    return;
  }
  
  tbody.innerHTML = metrics.map(m => {
    const fillRateClass = m.fill_rate_pct >= 95 ? 'text-green-400' : 
                          m.fill_rate_pct >= 80 ? 'text-yellow-400' : 
                          'text-red-400';
    
    const slippageClass = Math.abs(m.avg_slippage_pct) < 0.1 ? 'text-green-400' :
                          Math.abs(m.avg_slippage_pct) < 0.5 ? 'text-yellow-400' :
                          'text-red-400';
    
    return `
      <tr class="hover:bg-gray-700">
        <td class="table-cell font-semibold">${esc(m.symbol)}</td>
        <td class="table-cell text-right">${m.total_orders}</td>
        <td class="table-cell text-right text-green-400">${m.filled_orders}</td>
        <td class="table-cell text-right text-yellow-400">${m.canceled_orders}</td>
        <td class="table-cell text-right ${fillRateClass} font-semibold">${m.fill_rate_pct.toFixed(1)}%</td>
        <td class="table-cell text-right">${m.avg_time_to_fill_sec.toFixed(2)}s</td>
        <td class="table-cell text-right ${slippageClass}">${m.avg_slippage_pct.toFixed(3)}%</td>
      </tr>
    `;
  }).join('');
  
  // Add totals row
  if (totals) {
    const totalFillRateClass = totals.avg_fill_rate_pct >= 95 ? 'text-green-400' : 
                               totals.avg_fill_rate_pct >= 80 ? 'text-yellow-400' : 
                               'text-red-400';
    
    tbody.innerHTML += `
      <tr class="bg-gray-700 font-bold border-t-2 border-cyan-500">
        <td class="table-cell text-cyan-400">TOTAL</td>
        <td class="table-cell text-right">${totals.total_orders}</td>
        <td class="table-cell text-right text-green-400">${totals.filled_orders}</td>
        <td class="table-cell text-right text-yellow-400">${totals.canceled_orders}</td>
        <td class="table-cell text-right ${totalFillRateClass} text-lg">${totals.avg_fill_rate_pct.toFixed(1)}%</td>
        <td class="table-cell text-right">${totals.avg_time_to_fill_sec.toFixed(2)}s</td>
        <td class="table-cell text-right">${totals.avg_slippage_pct.toFixed(3)}%</td>
      </tr>
    `;
  }
}

function updateMetricsSummary(totals) {
  document.getElementById('metrics-fill-rate').textContent = `${totals.avg_fill_rate_pct.toFixed(1)}%`;
  document.getElementById('metrics-avg-time').textContent = `${totals.avg_time_to_fill_sec.toFixed(2)}s`;
  document.getElementById('metrics-avg-slippage').textContent = `${totals.avg_slippage_pct.toFixed(3)}%`;
  
  // Color code
  const fillRateEl = document.getElementById('metrics-fill-rate');
  fillRateEl.className = totals.avg_fill_rate_pct >= 95 ? 'text-green-400 text-2xl font-bold' :
                         totals.avg_fill_rate_pct >= 80 ? 'text-yellow-400 text-2xl font-bold' :
                         'text-red-400 text-2xl font-bold';
}

// ============================================================================
// 5. MASTER LOAD FUNCTION - Load all Alpaca data
// ============================================================================
async function loadAllAlpacaData() {
  if (!kpiWalletId) {
    console.error('No wallet selected');
    return;
  }
  
  // Show loading state
  showLoadingState();
  
  try {
    // Load all data in parallel
    await Promise.all([
      loadPortfolioHistory('1M'),
      loadAccountActivities('FILL'),
      loadClosedPositions(),
      loadOrderMetrics()
    ]);
    
    hideLoadingState();
  } catch (e) {
    console.error('Failed to load Alpaca data:', e);
    hideLoadingState();
  }
}

function showLoadingState() {
  // Add loading indicators to each section
  const sections = [
    'equity-curve-chart',
    'activities-table-body',
    'closed-positions-body',
    'order-metrics-body'
  ];
  
  sections.forEach(id => {
    const el = document.getElementById(id);
    if (el && el.tagName === 'TBODY') {
      el.innerHTML = '<tr><td colspan="10" class="table-cell text-center text-gray-400">Loading...</td></tr>';
    }
  });
}

function hideLoadingState() {
  // Loading indicators will be replaced by actual data
}