import { createCanvas } from '@napi-rs/canvas';

import type { OiData, OiStrike } from './oi-client.js';

const BG = '#0d1117';
const PANEL_BG = '#161b22';
const CALL_COLOR = '#4caf50';
const PUT_COLOR = '#ef5350';
const ATM_AMBER = '#ffb300';
const TEXT_MAIN = '#e6edf3';
const TEXT_DIM = '#8b949e';
const GRID = '#21262d';
const CALL_DIM = '#a5d6a7';
const PUT_DIM = '#ef9a9a';

const W = 1100;
const CX = W / 2;
const LABEL_W = 150;
const BAR_AREA = (W - LABEL_W) / 2; // 475 px per side

const BAR_H = 30;
const ROW_H = 44;
const HEADER_H = 90;
const COL_H = 34;
const FOOTER_H = 46;

const MAX_VISIBLE = 25;

const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

function fmtBig(v: number): string {
  if (v >= 1e9) return `$${(v / 1e9).toFixed(2)}B`;
  if (v >= 1e6) return `$${(v / 1e6).toFixed(0)}M`;
  if (v >= 1e3) return `$${(v / 1e3).toFixed(0)}K`;
  return `$${v.toFixed(0)}`;
}

function fmtStrikeTick(v: number): string {
  if (v >= 1_000_000) return `$${(v / 1_000_000).toFixed(1)}M`;
  if (v >= 1_000) return `$${(v / 1_000).toFixed(0)}K`;
  return `$${v}`;
}

function pickStrikes(all: OiStrike[], spot: number | null): OiStrike[] {
  if (all.length <= MAX_VISIBLE) return all;
  const pool = spot != null ? all.filter((s) => Math.abs(s.strike - spot) / spot <= 0.5) : [];
  const source = pool.length >= 5 ? pool : all;
  return source
    .slice()
    .sort((a, b) => b.callOiUsd + b.putOiUsd - (a.callOiUsd + a.putOiUsd))
    .slice(0, MAX_VISIBLE)
    .sort((a, b) => a.strike - b.strike);
}

export function renderOiChart(data: OiData): Buffer {
  const strikes = pickStrikes(data.strikes, data.spotPrice);
  const n = strikes.length;
  const H = HEADER_H + COL_H + n * ROW_H + FOOTER_H;

  const canvas = createCanvas(W, H);
  const ctx = canvas.getContext('2d');

  ctx.fillStyle = BG;
  ctx.fillRect(0, 0, W, H);

  ctx.fillStyle = PANEL_BG;
  ctx.fillRect(0, 0, W, HEADER_H);

  const [yr, mo, dy] = data.expiry.split('-');
  const monName = MONTHS[Number(mo) - 1] ?? mo ?? '';
  const expLabel = `${Number(dy)} ${monName} ${(yr ?? '').slice(2)}`;
  const spotLabel = data.spotPrice != null ? `  ·  Spot ${fmtBig(data.spotPrice)}` : '';

  ctx.textAlign = 'center';
  ctx.font = 'bold 24px sans-serif';
  ctx.fillStyle = TEXT_MAIN;
  ctx.fillText(`${data.underlying} Open Interest  ·  ${expLabel}${spotLabel}`, CX, 34);

  const pcRatio =
    data.totalCallOiUsd > 0 ? (data.totalPutOiUsd / data.totalCallOiUsd).toFixed(2) : '—';
  ctx.font = '17px sans-serif';
  ctx.fillStyle = TEXT_DIM;
  ctx.fillText(
    `Calls ${fmtBig(data.totalCallOiUsd)}  ·  Puts ${fmtBig(data.totalPutOiUsd)}  ·  P/C ${pcRatio}`,
    CX,
    64,
  );

  const colY = HEADER_H + COL_H - 10;
  ctx.font = 'bold 15px sans-serif';

  ctx.textAlign = 'right';
  ctx.fillStyle = PUT_DIM;
  ctx.fillText('← Puts', CX - LABEL_W / 2 - 10, colY);

  ctx.textAlign = 'center';
  ctx.fillStyle = TEXT_DIM;
  ctx.fillText('Strike', CX, colY);

  ctx.textAlign = 'left';
  ctx.fillStyle = CALL_DIM;
  ctx.fillText('Calls →', CX + LABEL_W / 2 + 10, colY);

  ctx.strokeStyle = GRID;
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(0, HEADER_H + COL_H);
  ctx.lineTo(W, HEADER_H + COL_H);
  ctx.stroke();

  if (n === 0) {
    ctx.textAlign = 'center';
    ctx.fillStyle = TEXT_DIM;
    ctx.fillText('No OI data available', CX, HEADER_H + COL_H + 50);
    return canvas.toBuffer('image/png');
  }

  let atmStrike: number | null = null;
  if (data.spotPrice != null) {
    const spot = data.spotPrice;
    let best: OiStrike | null = null;
    for (const s of strikes) {
      if (best == null || Math.abs(s.strike - spot) < Math.abs(best.strike - spot)) best = s;
    }
    atmStrike = best?.strike ?? null;
  }

  const maxOi = Math.max(...strikes.map((s) => Math.max(s.callOiUsd, s.putOiUsd)));
  const dataTop = HEADER_H + COL_H;
  const leftEdge = CX - LABEL_W / 2;
  const rightEdge = CX + LABEL_W / 2;

  for (const [i, s] of strikes.entries()) {
    const rowY = dataTop + i * ROW_H;
    const midY = rowY + ROW_H / 2;
    const barTop = midY - BAR_H / 2;
    const isAtm = s.strike === atmStrike;

    if (isAtm) {
      ctx.fillStyle = 'rgba(255,179,0,0.07)';
      ctx.fillRect(0, rowY, W, ROW_H);
    } else if (i % 2 === 1) {
      ctx.fillStyle = 'rgba(255,255,255,0.02)';
      ctx.fillRect(0, rowY, W, ROW_H);
    }

    ctx.strokeStyle = GRID;
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(leftEdge, rowY);
    ctx.lineTo(leftEdge, rowY + ROW_H);
    ctx.moveTo(rightEdge, rowY);
    ctx.lineTo(rightEdge, rowY + ROW_H);
    ctx.stroke();

    if (s.putOiUsd > 0 && maxOi > 0) {
      const bw = (s.putOiUsd / maxOi) * BAR_AREA;
      const bx = leftEdge - bw;
      ctx.fillStyle = PUT_COLOR;
      ctx.fillRect(bx, barTop, bw, BAR_H);
      if (bw >= 2) {
        ctx.fillStyle = 'rgba(255,255,255,0.18)';
        ctx.fillRect(bx, barTop, Math.min(2, bw), BAR_H);
      }
      ctx.font = 'bold 14px sans-serif';
      if (bw > 70) {
        ctx.fillStyle = TEXT_MAIN;
        ctx.textAlign = 'left';
        ctx.fillText(fmtBig(s.putOiUsd), bx + 6, midY + 5);
      } else if (bw > 5) {
        ctx.fillStyle = PUT_DIM;
        ctx.textAlign = 'right';
        ctx.fillText(fmtBig(s.putOiUsd), bx - 5, midY + 5);
      }
    }

    if (s.callOiUsd > 0 && maxOi > 0) {
      const bw = (s.callOiUsd / maxOi) * BAR_AREA;
      ctx.fillStyle = CALL_COLOR;
      ctx.fillRect(rightEdge, barTop, bw, BAR_H);
      if (bw >= 2) {
        ctx.fillStyle = 'rgba(255,255,255,0.18)';
        ctx.fillRect(rightEdge + bw - Math.min(2, bw), barTop, Math.min(2, bw), BAR_H);
      }
      ctx.font = 'bold 14px sans-serif';
      if (bw > 70) {
        ctx.fillStyle = TEXT_MAIN;
        ctx.textAlign = 'right';
        ctx.fillText(fmtBig(s.callOiUsd), rightEdge + bw - 6, midY + 5);
      } else if (bw > 5) {
        ctx.fillStyle = CALL_DIM;
        ctx.textAlign = 'left';
        ctx.fillText(fmtBig(s.callOiUsd), rightEdge + bw + 5, midY + 5);
      }
    }

    ctx.textAlign = 'center';
    ctx.font = isAtm ? 'bold 16px sans-serif' : '15px sans-serif';
    ctx.fillStyle = isAtm ? ATM_AMBER : TEXT_MAIN;
    ctx.fillText(fmtStrikeTick(s.strike), CX, midY + 6);

    if (isAtm) {
      ctx.font = 'bold 11px sans-serif';
      ctx.fillStyle = ATM_AMBER;
      ctx.fillText('ATM', CX, barTop - 2);
    }
  }

  ctx.strokeStyle = GRID;
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(0, dataTop + n * ROW_H);
  ctx.lineTo(W, dataTop + n * ROW_H);
  ctx.stroke();

  ctx.textAlign = 'center';
  ctx.font = '13px sans-serif';
  ctx.fillStyle = TEXT_DIM;
  ctx.fillText(`OI · notional USD · ${data.venueLabel}`, CX, dataTop + n * ROW_H + 28);

  return canvas.toBuffer('image/png');
}
