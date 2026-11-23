"use strict";

const byId = (id) => document.getElementById(id);
const timeZone = "Asia/Shanghai";
const integer = new Intl.NumberFormat("zh-CN");
const decimal = new Intl.NumberFormat("zh-CN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const energy = new Intl.NumberFormat("zh-CN", { maximumFractionDigits: 3 });
const localParts = new Intl.DateTimeFormat("en-CA", {
  timeZone, year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", hourCycle: "h23",
});
const readableTime = new Intl.DateTimeFormat("zh-CN", { timeZone, dateStyle: "short", timeStyle: "short" });
const statuses = [
  ["COMPLETED", "已完成", "completed"],
  ["REFUNDED", "已退款", "refunded"],
  ["CHARGING", "充电中", "charging"],
  ["ABNORMAL_CLOSED", "异常关闭", "abnormal"],
];
let currentRequest;

async function getJSON(path, signal) {
  const response = await fetch(path, { signal, cache: "no-store" });
  if (!response.ok) {
    const error = new Error(`HTTP ${response.status}`);
    error.status = response.status;
    throw error;
  }
  return response.json();
}

function renderDaily(rows, message) {
  const totals = rows.reduce((sum, row) => [
    sum[0] + row.order_count, sum[1] + row.energy_wh,
    sum[2] + row.electricity_fee_cents, sum[3] + row.service_fee_cents,
  ], [0, 0, 0, 0]);
  const format = ([orders, wh, electricity, service]) => [
    integer.format(orders), energy.format(wh / 1000),
    decimal.format(electricity / 100), decimal.format(service / 100),
  ];
  const formattedTotals = format(totals);
  ["orders", "energy", "electricity", "service"].forEach((id, index) => {
    byId(id).textContent = message ? "—" : formattedTotals[index];
  });
  byId("station-rows").replaceChildren(...rows.map((row) => {
    const tr = document.createElement("tr");
    const values = [
      row.station_id,
      ...format([row.order_count, row.energy_wh, row.electricity_fee_cents, row.service_fee_cents]),
    ];
    for (const value of values) tr.insertCell().textContent = value;
    return tr;
  }));
  byId("report-state").textContent = message || (rows.length ? "" : "该业务日暂无站点日报。");
}

function renderStream(rows, day, message) {
  const hours = Array.from({ length: 24 }, () => Object.fromEntries(statuses.map(([key]) => [key, 0])));
  const totals = { ...hours[0] };
  for (const row of rows) {
    const parts = Object.fromEntries(localParts.formatToParts(new Date(row.window_start_utc))
      .map(({ type, value }) => [type, value]));
    if (`${parts.year}-${parts.month}-${parts.day}` !== day || !(row.status in totals)) continue;
    hours[Number(parts.hour)][row.status] += row.event_count;
    totals[row.status] += row.event_count;
  }
  const count = Object.values(totals).reduce((a, b) => a + b, 0);
  const peak = Math.max(1, ...hours.map((hour) => Object.values(hour).reduce((a, b) => a + b, 0)));
  byId("event-count").textContent = message ? "—" : integer.format(count);
  byId("stream-state").textContent = message || (count ? "" : "该业务日暂无流事件。");
  byId("chart").hidden = Boolean(message);
  byId("chart").replaceChildren(...hours.map((counts, hour) => {
    const column = document.createElement("div");
    column.className = "hour";
    const label = `${String(hour).padStart(2, "0")}:00`;
    column.title = [label, ...statuses.filter(([key]) => counts[key]).map(([key, name]) => `${name} ${counts[key]}`)].join(" · ");
    if (Object.values(counts).some(Boolean)) {
      column.setAttribute("aria-label", column.title);
      column.setAttribute("role", "img");
    }
    for (const [key, , type] of statuses) {
      if (!counts[key]) continue;
      const bar = document.createElement("div");
      bar.className = `bar ${type}`;
      bar.style.height = `${counts[key] / peak * 100}%`;
      column.append(bar);
    }
    return column;
  }));
  byId("legend").replaceChildren(...statuses.map(([key, label, type]) => {
    const item = document.createElement("li");
    const swatch = document.createElement("span");
    swatch.className = type;
    item.append(swatch, `${label} ${message ? "—" : integer.format(totals[key])}`);
    return item;
  }));
}

function renderQuality(sources, message) {
  byId("quality-state").textContent = message || (sources.length ? "" : "暂无采集检查记录。");
  byId("source-rows").replaceChildren(...sources.map((source) => {
    const item = document.createElement("li");
    const name = document.createElement("strong");
    const state = document.createElement("span");
    const checked = document.createElement("small");
    const blocked = source.status === "blocked";
    const reasons = { contract_validation: "契约校验失败", version_conflict: "版本冲突" };
    name.textContent = source.source_key;
    state.textContent = blocked ? "受阻" : "通过";
    state.className = blocked ? "blocked" : "pass";
    const detail = blocked ? ` · ${reasons[source.failure_code] || "来源校验失败"}` : "";
    checked.textContent = `${readableTime.format(new Date(source.checked_at_utc))}${detail}`;
    item.append(name, state, checked);
    return item;
  }));
}

function renderForecast(result, failed) {
  const predictions = result?.predictions || [];
  const hasPredictions = predictions.length > 0;
  const total = predictions.reduce((sum, row) => sum + row.predicted_energy_wh, 0);
  byId("forecast-total").textContent = hasPredictions ? energy.format(total / 1000) : "—";
  byId("forecast-window").textContent = result
    ? `${readableTime.format(new Date(result.target_start_utc))} 至 ${readableTime.format(new Date(result.target_end_utc))} · 上海时间 · 24 小时基线`
    : "";
  byId("forecast-state").textContent = failed ? "预测暂不可用。"
    : hasPredictions ? "" : "过去 24 小时暂无可用于预测的站点订单。";
  byId("forecast-table").hidden = !hasPredictions;
  byId("forecast-rows").replaceChildren(...predictions.map((row) => {
    const tr = document.createElement("tr");
    tr.insertCell().textContent = row.station_id;
    tr.insertCell().textContent = energy.format(row.predicted_energy_wh / 1000);
    return tr;
  }));
}

async function refresh() {
  if (currentRequest) currentRequest.abort();
  const request = new AbortController();
  currentRequest = request;
  const day = byId("day").value;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(day) || day < "1970-01-02") {
    byId("load-status").textContent = "请选择 1970-01-02 之后的业务日。";
    return;
  }
  const utcMidnight = new Date(`${day}T00:00:00Z`);
  const previousUtcDay = new Date(utcMidnight.getTime() - 86400000).toISOString().slice(0, 10);
  const utcDays = [previousUtcDay, day];
  byId("load-status").textContent = "正在读取数据…";
  const get = (path) => getJSON(path, request.signal);
  const results = await Promise.allSettled([
    get(`/v1/demo/station-daily?day=${day}`),
    Promise.all(utcDays.map((utcDay) => get(`/v1/demo/billing-events/windows?day=${utcDay}`))),
    get("/v1/demo/collection-quality"),
    get("/v1/demo/station-forecast"),
  ]);
  if (request.signal.aborted) return;
  const [daily, stream, quality, prediction] = results;
  renderDaily(daily.value?.rows || [], daily.status === "rejected" ? "站点日报暂不可用。" : "");
  renderStream(stream.value?.flatMap((item) => item.rows) || [], day, stream.status === "rejected" ? "流窗口暂不可用。" : "");
  const qualityMessage = quality.status === "rejected"
    ? quality.reason.status === 403 ? "当前账户无权查看全局采集质量。" : "采集质量暂不可用。"
    : "";
  renderQuality(quality.value?.sources || [], qualityMessage);
  renderForecast(prediction.value, prediction.status === "rejected");
  const dateText = `业务日 ${day} · 流窗口查询 UTC ${utcDays.join(" / ")}`;
  byId("load-status").textContent = results.some(({ status, reason }) => status === "rejected" && reason.status !== 403)
    ? `${dateText} · 部分数据读取失败` : dateText;
}

byId("filters").addEventListener("submit", (event) => {
  event.preventDefault();
  refresh();
});
refresh();
