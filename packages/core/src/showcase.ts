const TRUE_VALUES = new Set(['1', 'true', 'yes', 'on']);

function isTruthyFlag(value: string | undefined): boolean {
  if (!value) return false;
  return TRUE_VALUES.has(value.trim().toLowerCase());
}

export function isShowcaseDemoEnabled(): boolean {
  return isTruthyFlag(process.env.NEXUS_SHOWCASE_DEMO);
}

export function isShowcaseDangerousToolsAllowed(): boolean {
  return isTruthyFlag(process.env.NEXUS_SHOWCASE_ALLOW_DANGEROUS_TOOLS);
}

export function assertShowcaseDemoEnabled(serviceName: string): void {
  if (isShowcaseDemoEnabled()) return;
  throw new Error(
    `[${serviceName}] Refusing to start: this public repository is showcase-only. Set NEXUS_SHOWCASE_DEMO=true to run a demo instance.`,
  );
}

