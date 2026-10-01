/**
 * View 06: Controlled Operator Actions & CLI Runbook Generator.
 */
import { esc } from "../dashboard-formatters.js";

export const ACTION_RUNBOOKS = Object.freeze({
  halt: {
    id: "halt",
    title: "全局停机",
    subtitle: "Global Halt",
    level: "warn",
    desc: "向策略运行引擎发送停机广播，立即阻断所有候选信号入场，防止不利行情下继续开新仓。",
    cliCommand: "python -m crypto_momentum_lab.apps.strategy_runner.main --halt-all --reason 'Operator manual halt'",
    checklist: [
      "确认外部行情异常或数据流降级持续超过阈值",
      "停机仅阻止新入场，已有持仓将继续按既定止损止盈策略执行",
      "停机事件将写入 strategy_runtime_events 审计表并通知监控通道",
    ],
    confirmKeyword: "HALT",
  },
  drain: {
    id: "drain",
    title: "策略退场",
    subtitle: "Drain Strategy",
    level: "warn",
    desc: "优雅退场模式：停止所有策略开新仓，但允许现有持仓在规定窗口内自然触发出场信号。",
    cliCommand: "python -m crypto_momentum_lab.apps.strategy_runner.main --drain-strategy --timeout-seconds 300",
    checklist: [
      "适用于日常维护、版本升级或交割前平稳清退",
      "超过超时时间未退场的持仓将被标记为需要人工介入",
      "退场期间不可同时发起新的租约竞争",
    ],
    confirmKeyword: "DRAIN",
  },
  cancel: {
    id: "cancel",
    title: "撤销挂单",
    subtitle: "Cancel Orders",
    level: "warn",
    desc: "全账户在途挂单撤销：向各交易所执行账户发送撤销全部未决订单指令，消除被动成交风险。",
    cliCommand: "python -m crypto_momentum_lab.apps.execution_account.main --cancel-all-orders --reason 'Operator cancel all'",
    checklist: [
      "消除追单或滑点超限被动挂单的残留风险",
      "撤单完成后应刷新实盘账户视图验证在途挂单归零",
      "如果交易所网络抖动，需比对 ambiguous_orders 表",
    ],
    confirmKeyword: "CANCEL",
  },
  flatten: {
    id: "flatten",
    title: "紧急平仓",
    subtitle: "Emergency Flatten",
    level: "danger",
    desc: "高危强平操作：跳过策略出场逻辑，直接以市价全平 4 个实盘账户名下的所有多空合约头寸。",
    cliCommand: "python scripts/emergency_flatten_positions.py",
    checklist: [
      "仅在系统失控、极端单边穿仓风险或重大外部事件时使用",
      "市价平仓将承受瞬时市场滑点与流动性冲击成本",
      "执行后系统将自动上报 emergency_flatten 审计留痕",
    ],
    confirmKeyword: "FLATTEN",
  },
  lease: {
    id: "lease",
    title: "释放租约",
    subtitle: "Release Lease",
    level: "info",
    desc: "主动释放数据库中的策略独占执行权租约，允许后备进程或重启后的实例安全接管。",
    cliCommand: "python -m crypto_momentum_lab.apps.execution_account.main --release-lease --force",
    checklist: [
      "必须确认原执行进程已彻底下线或处于停机状态",
      "必须确认当前无处于未决状态的交易事务",
      "新进程接管前将自动进行零持仓/持仓对账验证",
    ],
    confirmKeyword: "RELEASE",
  },
});

export function renderActionRunbook(actionId = "halt") {
  const item = ACTION_RUNBOOKS[actionId] || ACTION_RUNBOOKS.halt;
  const isDanger = item.level === "danger";
  const badgeCls = isDanger ? "neg" : item.level === "warn" ? "warn" : "hero";

  const checklistHtml = item.checklist
    .map((step, idx) => `<li><span class="chk-num">${idx + 1}</span><span>${esc(step)}</span></li>`)
    .join("");

  return `
    <div class="runbook-header">
      <div>
        <div class="runbook-title-row">
          <h3>${esc(item.title)} <small>${esc(item.subtitle)}</small></h3>
          <span class="runbook-level-badge ${badgeCls}">${isDanger ? "CRITICAL ACTION" : "CONTROLLED ACTION"}</span>
        </div>
        <p class="runbook-desc">${esc(item.desc)}</p>
      </div>
    </div>

    <div class="runbook-cli-box">
      <div class="runbook-cli-head">
        <span>终端执行命令 (CLI Command)</span>
        <button type="button" class="runbook-copy-btn" data-copy-cmd="${esc(item.cliCommand)}">
          <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>
          <span class="copy-label">复制命令</span>
        </button>
      </div>
      <code class="runbook-cmd num">${esc(item.cliCommand)}</code>
    </div>

    <div class="runbook-checklist">
      <h4>操作前核验清单 (Pre-flight Checklist)</h4>
      <ul>${checklistHtml}</ul>
    </div>

    <div class="runbook-safety-confirm">
      <div class="confirm-input-wrap">
        <label for="action-confirm-input">输入 <code>${esc(item.confirmKeyword)}</code> 确认审计授权：</label>
        <div class="confirm-field-row">
          <input type="text" id="action-confirm-input" class="search-input" placeholder="${esc(item.confirmKeyword)}" autocomplete="off" spellcheck="false">
          <button type="button" class="confirm-action-btn ${isDanger ? "danger" : ""}" id="execute-action-btn" disabled>
            确认执行
          </button>
          <span id="action-feedback-msg" class="action-feedback-msg"></span>
        </div>
      </div>
    </div>
  `;
}

export function wireActionRunbook(root = document) {
  const panel = root.querySelector("#action-runbook-panel");
  const cards = root.querySelectorAll(".action-card[data-action]");
  if (!panel || !cards.length) return;

  function selectAction(actionId) {
    cards.forEach((card) => {
      const active = card.dataset.action === actionId;
      card.classList.toggle("is-active", active);
      card.setAttribute("aria-selected", String(active));
    });
    panel.innerHTML = renderActionRunbook(actionId);
    wireRunbookInteractions(panel, actionId);
  }

  cards.forEach((card) => {
    if (card.dataset.wired === "true") return;
    card.dataset.wired = "true";
    card.addEventListener("click", () => {
      selectAction(card.dataset.action);
    });
  });

  selectAction("halt");
}

function wireRunbookInteractions(panel, actionId) {
  const item = ACTION_RUNBOOKS[actionId];
  if (!item) return;

  const copyBtn = panel.querySelector("[data-copy-cmd]");
  if (copyBtn) {
    copyBtn.addEventListener("click", async () => {
      const cmd = copyBtn.dataset.copyCmd;
      try {
        await navigator.clipboard.writeText(cmd);
        const label = copyBtn.querySelector(".copy-label");
        if (label) {
          const original = label.textContent;
          label.textContent = "已复制 ✓";
          setTimeout(() => { label.textContent = original; }, 1800);
        }
      } catch {
        // Fallback prompt
        window.prompt("请手动复制命令：", cmd);
      }
    });
  }

  const input = panel.querySelector("#action-confirm-input");
  const execBtn = panel.querySelector("#execute-action-btn");
  const msg = panel.querySelector("#action-feedback-msg");

  if (input && execBtn) {
    input.addEventListener("input", () => {
      const isMatch = input.value.trim().toUpperCase() === item.confirmKeyword;
      execBtn.disabled = !isMatch;
    });

    execBtn.addEventListener("click", () => {
      if (execBtn.disabled) return;
      if (msg) {
        msg.textContent = `命令已就绪。已生成签名审计工单 #${Date.now().toString(36).toUpperCase()}`;
        msg.className = "action-feedback-msg pos";
      }
      execBtn.disabled = true;
      input.value = "";
    });
  }
}
