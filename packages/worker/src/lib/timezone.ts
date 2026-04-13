/**
 * Timezone utilities. All human-facing formatting uses Eastern Time.
 */

export const TIMEZONE = 'America/New_York';

export function formatDateET(date: Date, options?: Intl.DateTimeFormatOptions): string {
  return date.toLocaleDateString('en-US', { timeZone: TIMEZONE, ...options });
}

export function formatTimeET(date: Date, options?: Intl.DateTimeFormatOptions): string {
  return date.toLocaleTimeString('en-US', { timeZone: TIMEZONE, ...options });
}

export function formatDateTimeET(date: Date, options?: Intl.DateTimeFormatOptions): string {
  return date.toLocaleString('en-US', { timeZone: TIMEZONE, ...options });
}

export function todayET(now = new Date()): string {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: TIMEZONE, year: 'numeric', month: '2-digit', day: '2-digit',
  }).formatToParts(now);
  const y = parts.find(p => p.type === 'year')!.value;
  const m = parts.find(p => p.type === 'month')!.value;
  const d = parts.find(p => p.type === 'day')!.value;
  return `${y}-${m}-${d}`;
}

export function hourET(now = new Date()): number {
  return parseInt(
    new Intl.DateTimeFormat('en-US', { timeZone: TIMEZONE, hour: 'numeric', hour12: false }).format(now), 10,
  );
}

export function todayBoundsET(now = new Date()): { start: Date; end: Date } {
  const dateStr = todayET(now);
  const offset = etOffsetString(now);
  return {
    start: new Date(`${dateStr}T00:00:00-${offset}`),
    end: new Date(`${dateStr}T23:59:59.999-${offset}`),
  };
}

function etOffsetString(now = new Date()): string {
  const fmt = new Intl.DateTimeFormat('en-US', { timeZone: TIMEZONE, timeZoneName: 'shortOffset' });
  const parts = fmt.formatToParts(now);
  const tzPart = parts.find(p => p.type === 'timeZoneName')?.value ?? 'GMT-5';
  const match = tzPart.match(/GMT([+-]\d+)/);
  const offsetHours = match ? Math.abs(parseInt(match[1])) : 5;
  return `${String(offsetHours).padStart(2, '0')}:00`;
}
