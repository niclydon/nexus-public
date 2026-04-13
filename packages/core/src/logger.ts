const LOG_LEVELS = { minimal: 0, normal: 1, verbose: 2, debug: 3 } as const;
type LogLevel = keyof typeof LOG_LEVELS;

const currentLevel = LOG_LEVELS[(process.env.LOG_LEVEL as LogLevel) ?? 'normal'] ?? 1;

export function createLogger(component: string) {
  const prefix = `[${component}]`;
  const timers = new Map<string, number>();

  return {
    logMinimal(...args: unknown[]) {
      console.log(prefix, ...args);
    },

    log(...args: unknown[]) {
      if (currentLevel >= 1) console.log(prefix, ...args);
    },

    logVerbose(...args: unknown[]) {
      if (currentLevel >= 2) console.log(prefix, ...args);
    },

    logDebug(...args: unknown[]) {
      if (currentLevel >= 3) console.log(prefix, ...args);
    },

    time(label: string): () => void {
      const start = performance.now();
      return () => {
        const ms = Math.round(performance.now() - start);
        if (currentLevel >= 2) console.log(prefix, `${label}: ${ms}ms`);
      };
    },
  };
}
