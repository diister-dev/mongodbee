import { createWarmState } from "./interaction.ts";

export const hoverWarmth = createWarmState(400);

let current = null;

export function claimCard(close) {
  if (current && current !== close) current();
  current = close;
}

export function releaseCard(close) {
  if (current === close) current = null;
}

export const tipWarmth = createWarmState(300);

let currentTip = null;

export function claimTip(close) {
  if (currentTip && currentTip !== close) currentTip();
  currentTip = close;
}

export function releaseTip(close) {
  if (currentTip === close) currentTip = null;
}
