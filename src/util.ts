import { createHash, randomUUID } from "node:crypto";
import type { JsonValue, QuietHoursConfig } from "./types.js";

export const DEFAULT_TIMEZONE = "UTC";

export function id(prefix: string): string {
  return `${prefix}_${randomUUID()}`;
}

export function isoNow(now = new Date()): string {
  return now.toISOString();
}

export function asDate(value: string | Date): Date {
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) throw new Error(`Invalid date: ${String(value)}`);
  return date;
}

export function addMs(value: string | Date, milliseconds: number): string {
  return new Date(asDate(value).getTime() + milliseconds).toISOString();
}

export function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

export function parseJson<T>(value: unknown, fallback: T): T {
  if (typeof value !== "string") return (value as T) ?? fallback;
  try {
    return JSON.parse(value) as T;
  } catch {
    return fallback;
  }
}

export function stableJson(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  const object = value as Record<string, unknown>;
  return `{${Object.keys(object).sort().map((key) => `${JSON.stringify(key)}:${stableJson(object[key])}`).join(",")}}`;
}

export function getPath(value: unknown, path: string): unknown {
  return path.split(".").reduce<unknown>((current, key) => {
    if (current && typeof current === "object" && key in current) {
      return (current as Record<string, unknown>)[key];
    }
    return undefined;
  }, value);
}

export function localParts(date: Date, timezone: string): { date: string; hour: number; minute: number; second: number } {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: timezone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
  });
  const values = Object.fromEntries(formatter.formatToParts(date).map((part) => [part.type, part.value]));
  return {
    date: `${values.year}-${values.month}-${values.day}`,
    hour: Number(values.hour),
    minute: Number(values.minute),
    second: Number(values.second),
  };
}

export function localMinute(date: Date, timezone: string): number {
  const parts = localParts(date, timezone);
  return parts.hour * 60 + parts.minute;
}

export function parseClock(value: string): number {
  const match = /^(\d{1,2}):(\d{2})$/.exec(value.trim());
  if (!match) throw new Error(`Invalid local time: ${value}`);
  const hour = Number(match[1]);
  const minute = Number(match[2]);
  if (hour > 23 || minute > 59) throw new Error(`Invalid local time: ${value}`);
  return hour * 60 + minute;
}

export function clockInWindow(minute: number, start: number, end: number): boolean {
  if (start === end) return true;
  if (start < end) return minute >= start && minute < end;
  return minute >= start || minute < end;
}

export function quietWindowAt(date: Date, config: QuietHoursConfig | null | undefined, fallbackTimezone = DEFAULT_TIMEZONE): boolean {
  if (!config?.windows?.length) return false;
  const timezone = config.timezone || fallbackTimezone;
  const minute = localMinute(date, timezone);
  return config.windows.some((window) => clockInWindow(minute, parseClock(window.start), parseClock(window.end)));
}

/** Find the first minute at which a quiet-hours gate is open again. */
export function quietEndAfter(date: Date, config: QuietHoursConfig | null | undefined, fallbackTimezone = DEFAULT_TIMEZONE): Date {
  if (!quietWindowAt(date, config, fallbackTimezone)) return date;
  // A local clock window is at most 24 hours.  Scan in minute increments so
  // this remains correct across timezone offsets and midnight windows. Start
  // from an absolute minute boundary so a 01:45:37 observation releases at
  // 03:30:00, never 03:30:37.
  const start = date.getTime();
  const firstBoundary = Math.floor(start / 60_000) * 60_000;
  for (let minute = 1; minute <= 48 * 60 + 2; minute += 1) {
    const candidate = new Date(firstBoundary + minute * 60_000);
    if (!quietWindowAt(candidate, config, fallbackTimezone)) return candidate;
  }
  return new Date(start + 24 * 60 * 60_000);
}

/** Resolve the next occurrence of HH:mm in a timezone using a bounded search. */
export function nextLocalClock(date: Date, clock: string, timezone: string): Date {
  const target = parseClock(clock);
  const start = date.getTime();
  const firstBoundary = Math.floor(start / 60_000) * 60_000;
  for (let minute = 0; minute <= 48 * 60 + 2; minute += 1) {
    const candidate = new Date(firstBoundary + minute * 60_000);
    if (candidate.getTime() < start) continue;
    const parts = localParts(candidate, timezone);
    if (parts.hour * 60 + parts.minute === target && candidate.getTime() >= start) {
      return candidate;
    }
  }
  return new Date(start + 24 * 60 * 60_000);
}

export function jsonValue(value: unknown): JsonValue {
  return value as JsonValue;
}

export function assertFiniteMs(value: number | null | undefined, field: string, fallback: number): number {
  const resolved = value ?? fallback;
  if (!Number.isFinite(resolved) || resolved < 0) throw new Error(`${field} must be a non-negative finite number`);
  return resolved;
}
