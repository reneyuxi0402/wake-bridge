/**
 * Shape checks shared by Core, the owner surfaces and the public SDK entry
 * points.  Keep this module free of imports: `./event`, `./source` and
 * `./transport` load it too.
 */

/** Absolute timestamp with `Z` or a numeric offset; local times are rejected. */
export const ABSOLUTE_RFC3339 = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/u;

/** Source ids; instance and owner ids use the same shape. */
export const SOURCE_ID = /^[A-Za-z0-9_.:-]{1,128}$/u;

/** `HH:MM` in the agent's local timezone (quiet hours, scheduled policies). */
export const LOCAL_TIME = /^(?:[01]\d|2[0-3]):[0-5]\d$/u;

/** Minimum length for owner, host, source and route credentials. */
export const MIN_SECRET_LENGTH = 32;

/** Route address keys whose values are credentials and are redacted outside Core. */
export const SECRET_ADDRESS_KEY = /token|secret|authorization|bearer/i;
