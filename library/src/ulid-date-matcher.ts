/**
 * ULID Date Matcher
 * 
 * This module provides utilities to generate regular expressions that match ULID timestamps
 * based on date ranges. It allows filtering ULIDs by their embedded timestamp component
 * without having to decode each ULID individually.
 * 
 * ULIDs (Universally Unique Lexicographically Sortable Identifiers) consist of:
 * - A 10-character timestamp component (first 10 chars) that encodes milliseconds since Unix epoch
 * - A 16-character random component (last 16 chars)
 * 
 * This utility focuses on the timestamp part to create efficient regex patterns for time-based filtering.
 * 
 * @module ulid-date-matcher
 */

import { ulid } from "@std/ulid";

/**
 * Constants for ULID timestamp calculations
 * ULIDs use milliseconds since the Unix Epoch (1970-01-01) encoded in Crockford's Base32
 */
/** Base32 digit characters (0-9) */
const ENCODING_NUMBERS = "0123456789";

/** Base32 letter characters (A-Z) */
const ENCODING_LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

/** All characters used in Crockford's Base32 encoding (0-9A-Z) */
const ENCODING_CHARS = `${ENCODING_NUMBERS}${ENCODING_LETTERS}`;

/** Regex pattern matching any valid ULID character */
const ANY_ELEMENT = "[0-9A-Z]";

/** Number of characters in the timestamp portion of a ULID (first 10 chars) */
const TIMESTAMP_LENGTH = 10;

/** Total length of a complete ULID (26 characters) */
const ULID_LENGTH = 26;

/**
 * Maps each Base32 character to a regex pattern matching characters greater than it
 * Example: '5' maps to '[6-9A-Z]'
 */
const greaterMap: Record<string, string | undefined> = {};

/**
 * Maps each Base32 character to a regex pattern matching characters greater than or equal to it
 * Example: '5' maps to '[5-9A-Z]'
 */
const greaterEqMap: Record<string, string | undefined> = {};

/**
 * Maps each Base32 character to a regex pattern matching characters less than it
 * Example: '5' maps to '[0-4]'
 */
const lesserMap: Record<string, string | undefined> = {};

/**
 * Maps each Base32 character to a regex pattern matching characters less than or equal to it
 * Example: '5' maps to '[0-5]'
 */
const lesserEqMap: Record<string, string | undefined> = {};

for (let i = 0; i < ENCODING_CHARS.length; i++) {
  const char = ENCODING_CHARS[i];
  greaterMap[char] = greaterCharRegex(char);
  greaterEqMap[char] = greaterCharRegex(char, true);
  lesserMap[char] = lesserCharRegex(char);
  lesserEqMap[char] = lesserCharRegex(char, true);
}

/**
 * Generates a regex pattern for matching characters greater than (or equal to) the given character
 * in Base32 encoding.
 * 
 * @param char - The Base32 character (0-9A-Z) to compare against
 * @param equal - If true, includes the character itself in the pattern (>=), otherwise excludes it (>)
 * @returns A regex character class string like '[5-9A-Z]' or undefined if not possible
 */
function greaterCharRegex(char: string, equal = false) {
  const index = ENCODING_CHARS.indexOf(char);
  if (index === -1) {
    return undefined; // Invalid character
  }

  const startIndex = equal ? index : index + 1;
  const toInclude = ENCODING_CHARS.slice(startIndex);
  if(toInclude.length === 0) {
    return undefined; // No greater characters
  }

  let regex = "";
  if (startIndex < ENCODING_NUMBERS.length) {
    regex = `[${toInclude[0]}-9A-Z]`;
  } else {
    regex = `[${toInclude[0]}-Z]`;
  }
  
  return regex;
}

/**
 * Generates a regex pattern for matching characters less than (or equal to) the given character
 * in Base32 encoding.
 * 
 * @param char - The Base32 character (0-9A-Z) to compare against
 * @param equal - If true, includes the character itself in the pattern (<=), otherwise excludes it (<)
 * @returns A regex character class string like '[0-5]' or '[0-9A-F]' or undefined if not possible
 */
function lesserCharRegex(char: string, equal = false) {
  const index = ENCODING_CHARS.indexOf(char);
  if (index === -1) {
    return undefined; // Invalid character
  }

  const startIndex = equal ? index : index - 1;
  if (startIndex < 0) {
    return undefined; // No lesser characters
  }
  const toInclude = ENCODING_CHARS.slice(0, startIndex + 1);
  if(toInclude.length === 0) {
    return undefined; // No lesser characters
  }

  let regex = "";
  if (startIndex < ENCODING_NUMBERS.length) {
    regex = `[0-${toInclude[toInclude.length - 1]}]`;
  } else {
    regex = `[0-9A-${toInclude[toInclude.length - 1]}]`;
  }
  
  return regex;
}

/**
 * Options for configuring time-based ULID matching.
 * Requires at least one boundary (start or end date).
 */
type TimeMatcherOptions =
  | ({ start: Date, startEqual?: boolean } & Partial<{ end: Date, endEqual: boolean }>)
  | ({ end: Date, endEqual?: boolean } & Partial<{ start: Date, startEqual: boolean }>);

/**
 * Full set of options for the ULID matcher, extending TimeMatcherOptions with additional
 * configuration options.
 * 
 * @property prefix - Optional string prefix to match before the ULID (e.g., "user:")
 * @property start - Optional start date for the time range
 * @property end - Optional end date for the time range
 * @property startEqual - Whether to include ULIDs exactly at the start date (inclusive)
 * @property endEqual - Whether to include ULIDs exactly at the end date (inclusive)
 */
type MatcherOptions = {
  /** Optional string prefix to match before the ULID (e.g., "user:") */
  prefix?: string;
} & TimeMatcherOptions;

export function ulidTimeMatcher(options?: MatcherOptions) {
  const start = options?.start ? ulid(options.start.getTime()).slice(0, TIMESTAMP_LENGTH) : undefined;
  const end = options?.end ? ulid(options.end.getTime()).slice(0, TIMESTAMP_LENGTH) : undefined;

  const startEqual = options?.startEqual ?? false;
  const endEqual = options?.endEqual ?? false;
  const prefix = options?.prefix ?? "";
  const startRegex = start ? greaterUlidPattern(start, startEqual) : "";
  const endRegex = end ? lesserUlidPattern(end, endEqual) : "";
  const regex = `${prefix}(?=${startRegex})(?=${endRegex}).{${ULID_LENGTH}}`;
  return new RegExp(regex);
}

export function greaterUlidPattern(ulidTime: string, equal = false) {
  const map = equal ? greaterEqMap : greaterMap;
  return [...ulidTime].map((char, index) => {
    const invIndex = ulidTime.length - index - 1;
    const greater = invIndex == 0 ? map[char] : greaterMap[char];
    if (!greater) return undefined;
    const beforeChar = ulidTime.slice(0, index);
    return `${beforeChar}${greater}${ANY_ELEMENT}{${ulidTime.length - index - 1}}`;
  }).filter(Boolean).join("|");
}

export function lesserUlidPattern(ulidTime: string, equal = false) {
  const map = equal ? lesserEqMap : lesserMap;
  return [...ulidTime].map((char, index) => {
    const invIndex = ulidTime.length - index - 1;
    const lesser = invIndex == 0 ? map[char] : lesserMap[char];
    if (!lesser) return undefined;
    const beforeChar = ulidTime.slice(0, index);
    return `${beforeChar}${lesser}${ANY_ELEMENT}{${ulidTime.length - index - 1}}`;
  }).filter(Boolean).join("|");
}