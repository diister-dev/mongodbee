import { assertEquals, assertMatch, assertNotMatch } from "jsr:@std/assert";
import { ulid } from "@std/ulid";
import { 
  ulidTimeMatcher, 
  greaterUlidPattern, 
  lesserUlidPattern 
} from "../src/ulid-date-matcher.ts";

Deno.test("ULID Date Matcher: Basic Functionality", () => {
  // Create ULIDs for specific dates
  const date1 = new Date("2023-01-01T00:00:00.000Z");
  const date2 = new Date("2023-01-15T00:00:00.000Z");
  const date3 = new Date("2023-02-01T00:00:00.000Z");
  
  const ulid1 = ulid(date1.getTime());
  const ulid2 = ulid(date2.getTime());
  const ulid3 = ulid(date3.getTime());
  
  // Test matching ULIDs in a date range
  const matcher = ulidTimeMatcher({
    start: date1,
    end: date3,
  });
  
  // Should match ULIDs in the range
  assertMatch(ulid2, matcher);
  
  // Should match ULIDs at the boundaries when inclusive
  const inclusiveMatcher = ulidTimeMatcher({
    start: date1,
    end: date3,
    startEqual: true,
    endEqual: true,
  });
  
  assertMatch(ulid1, inclusiveMatcher);
  assertMatch(ulid3, inclusiveMatcher);
  
  // Should not match ULIDs at the boundaries when exclusive
  const exclusiveMatcher = ulidTimeMatcher({
    start: date1,
    end: date3,
    startEqual: false,
    endEqual: false,
  });
  
  assertNotMatch(ulid1, exclusiveMatcher);
  assertNotMatch(ulid3, exclusiveMatcher);
});

Deno.test("ULID Date Matcher: With Prefix", () => {
  const date1 = new Date("2023-01-01T00:00:00.000Z");
  const date3 = new Date("2023-02-01T00:00:00.000Z");
  
  const prefixedUlid = "user:" + ulid(date1.getTime() + 1000 * 60 * 60 * 24); // One day after date1
  const nonPrefixedUlid = ulid(date1.getTime() + 1000 * 60 * 60 * 24); // One day after date1
  
  // Test matching ULIDs with a prefix
  const prefixMatcher = ulidTimeMatcher({
    start: date1,
    end: date3,
    prefix: "user:",
  });
  
  assertMatch(prefixedUlid, prefixMatcher);
  assertNotMatch(nonPrefixedUlid, prefixMatcher);
});

Deno.test("ULID Date Matcher: Single Boundary Tests", () => {
  const now = new Date();
  const past = new Date(now.getTime() - 1000 * 60 * 60 * 24 * 30); // 30 days ago
  const future = new Date(now.getTime() + 1000 * 60 * 60 * 24 * 30); // 30 days in future
  
  const pastUlid = ulid(past.getTime());
  const nowUlid = ulid(now.getTime());
  const futureUlid = ulid(future.getTime());
  
  // Test only start date
  const afterPastMatcher = ulidTimeMatcher({
    start: past,
  });
  
  assertMatch(nowUlid, afterPastMatcher);
  assertMatch(futureUlid, afterPastMatcher);
  assertNotMatch(pastUlid, afterPastMatcher); // Exclusive by default
  
  // Test only end date
  const beforeFutureMatcher = ulidTimeMatcher({
    end: future,
  });
  
  assertMatch(pastUlid, beforeFutureMatcher);
  assertMatch(nowUlid, beforeFutureMatcher);
  assertNotMatch(futureUlid, beforeFutureMatcher); // Exclusive by default
});

Deno.test("ULID Date Matcher: Pattern Generation Functions", () => {
  const timestamp = ulid().slice(0, 10); // Get the timestamp part of a ULID
  
  // Test greater pattern generation
  const greaterPattern = greaterUlidPattern(timestamp);
  const greaterEqualPattern = greaterUlidPattern(timestamp, true);
  
  // Test lesser pattern generation
  const lesserPattern = lesserUlidPattern(timestamp);
  const lesserEqualPattern = lesserUlidPattern(timestamp, true);
  
  // Assert that patterns were generated (non-empty strings)
  assertEquals(typeof greaterPattern, "string");
  assertEquals(typeof greaterEqualPattern, "string");
  assertEquals(typeof lesserPattern, "string");
  assertEquals(typeof lesserEqualPattern, "string");
  
  // Assert that patterns are not empty
  assertEquals(greaterPattern.length > 0, true);
  assertEquals(greaterEqualPattern.length > 0, true);
  assertEquals(lesserPattern.length > 0, true);
  assertEquals(lesserEqualPattern.length > 0, true);
});

Deno.test("ULID Date Matcher: Edge Cases", () => {
  // Test with minimum date (close to epoch)
  const minDate = new Date(1);
  const minUlid = ulid(minDate.getTime());
  
  // Test with maximum date (far in the future)
  const maxDate = new Date(281474976710655); // Maximum valid JavaScript date
  const maxUlid = ulid(maxDate.getTime());
  
  // Create a matcher that should match everything
  const allMatcher = ulidTimeMatcher({
    start: minDate,
    end: maxDate,
    startEqual: true,
    endEqual: true,
  });
  
  // Should match both extreme dates
  assertMatch(minUlid, allMatcher);
  assertMatch(maxUlid, allMatcher);
  
  // Test with equal start and end dates
  const exactDate = new Date("2023-05-05T12:00:00.000Z");
  const exactUlid = ulid(exactDate.getTime());
  
  const exactMatcher = ulidTimeMatcher({
    start: exactDate,
    end: exactDate,
    startEqual: true,
    endEqual: true,
  });
  
  // Should only match ULIDs from that exact time
  assertMatch(exactUlid, exactMatcher);
});

Deno.test("ULID Date Matcher: Real-world Usage Scenario", () => {
  // Test a scenario where we want to find ULIDs created in the past hour
  const now = new Date();
  const oneHourAgo = new Date(now.getTime() - 1000 * 60 * 60);
  
  // Create ULIDs at different times
  const twoHoursAgoUlid = ulid(now.getTime() - 1000 * 60 * 60 * 2);
  const halfHourAgoUlid = ulid(now.getTime() - 1000 * 60 * 30);
  const justNowUlid = ulid();
  
  // Matcher for the past hour
  const pastHourMatcher = ulidTimeMatcher({
    start: oneHourAgo,
    end: now,
    startEqual: true,
    endEqual: true,
  });
  
  // Should match ULIDs created within the past hour
  assertMatch(halfHourAgoUlid, pastHourMatcher);
  assertMatch(justNowUlid, pastHourMatcher);
  assertNotMatch(twoHoursAgoUlid, pastHourMatcher);
});
