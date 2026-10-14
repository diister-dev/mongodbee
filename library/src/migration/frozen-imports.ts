/**
 * Enforces that migrations are frozen in time.
 *
 * A migration replays the schema it was written against, so it must import
 * nothing that keeps evolving after it ships: no application module, no path
 * alias, no relative file outside the migrations directory. Such an import
 * makes an old migration silently pick up today's code, and `check` would
 * validate a chain that no longer reads like the one that ran in production.
 *
 * The scanner reads the source text only — it never imports a file — with a
 * small lexer that skips comments, strings and template literals, so a
 * specifier quoted in a comment or a string is never mistaken for an import.
 * Import maps are deliberately NOT resolved: an alias such as `#app/`, `$lib`
 * or `@/` is exactly what points back into the application.
 *
 * @module
 */

import * as fs from "node:fs/promises";
import * as path from "node:path";

/** How `check` reacts to a migration that is not frozen. */
export type FrozenImportsLevel = "off" | "warn" | "error";

/** The resolved form of `migration.validation.frozen`. */
export interface FrozenImportsPolicy {
  level: FrozenImportsLevel;
  /** Allowed specifier prefixes, the defaults included. */
  allow: string[];
}

/**
 * What a migration may import without breaking its freeze: the runtime, the
 * library itself and the validation library its schemas are written in.
 *
 * An entry ending in `:` or `/` is a prefix; any other entry is a package
 * name, matching itself, its subpaths and its versioned forms (`x@1.2/sub`).
 */
export const DEFAULT_FROZEN_ALLOW: readonly string[] = [
  "node:",
  "@diister/mongodbee",
  "jsr:@diister/mongodbee",
  "npm:@diister/mongodbee",
  "valibot",
  "npm:valibot",
  "jsr:@valibot/valibot",
];

/** One import found in a source file. */
export interface ExtractedImport {
  /** The literal specifier, or the expression text of a non-literal `import()`. */
  specifier: string;
  line: number;
  kind: "import" | "side-effect" | "re-export" | "dynamic";
  /** `import type` / `export type … from`: erased at runtime. */
  typeOnly: boolean;
  /** `false` for an `import(expr)` whose target cannot be read statically. */
  literal: boolean;
}

/** A specifier that breaks the freeze. */
export interface FrozenImportOffender {
  /** File path relative to the migrations directory, `/`-separated. */
  file: string;
  line: number;
  specifier: string;
  reason: string;
  /** A type-only import is always a warning: it changes no runtime behaviour. */
  severity: "warning" | "error";
}

type Token =
  | { type: "ident"; value: string; line: number; start: number; end: number }
  | { type: "punct"; value: string; line: number; start: number; end: number }
  | {
      type: "string";
      value: string;
      line: number;
      start: number;
      end: number;
      /** A template literal with `${}` has no static value. */
      static: boolean;
    };

const REGEX_AFTER_KEYWORDS = new Set([
  "return",
  "typeof",
  "instanceof",
  "in",
  "of",
  "new",
  "delete",
  "void",
  "throw",
  "case",
  "do",
  "else",
  "yield",
  "await",
]);

function isIdentStart(c: string): boolean {
  return /[A-Za-z_$\u0080-￿]/.test(c);
}

function isIdentPart(c: string): boolean {
  return /[A-Za-z0-9_$\u0080-￿]/.test(c);
}

function tokenize(source: string): Token[] {
  const tokens: Token[] = [];
  // One entry per open `{`: whether it opened a template substitution, so the
  // matching `}` resumes the template instead of closing a block.
  const braces: boolean[] = [];
  let line = 1;
  let i = 0;
  const n = source.length;

  const regexAllowed = (): boolean => {
    const last = tokens[tokens.length - 1];
    if (!last) return true;
    if (last.type === "string") return false;
    if (last.type === "ident") return REGEX_AFTER_KEYWORDS.has(last.value);
    return ![")", "]", "}"].includes(last.value);
  };

  // Reads template text from `i` (just past a backtick or a closing `}`) up
  // to the closing backtick or the next `${`.
  const readTemplate = (start: number, startLine: number, first: boolean) => {
    let text = "";
    while (i < n) {
      const c = source[i];
      if (c === "\\") {
        text += source.slice(i, i + 2);
        if (source[i + 1] === "\n") line++;
        i += 2;
        continue;
      }
      if (c === "`") {
        i++;
        tokens.push({
          type: "string",
          value: text,
          line: startLine,
          start,
          end: i,
          static: first,
        });
        return;
      }
      if (c === "$" && source[i + 1] === "{") {
        i += 2;
        braces.push(true);
        tokens.push({
          type: "string",
          value: text,
          line: startLine,
          start,
          end: i,
          static: false,
        });
        return;
      }
      if (c === "\n") line++;
      text += c;
      i++;
    }
  };

  while (i < n) {
    const c = source[i];
    if (c === "\n") {
      line++;
      i++;
      continue;
    }
    if (c === " " || c === "\t" || c === "\r") {
      i++;
      continue;
    }
    if (c === "/" && source[i + 1] === "/") {
      while (i < n && source[i] !== "\n") i++;
      continue;
    }
    if (c === "/" && source[i + 1] === "*") {
      i += 2;
      while (i < n && !(source[i] === "*" && source[i + 1] === "/")) {
        if (source[i] === "\n") line++;
        i++;
      }
      i += 2;
      continue;
    }
    if (c === "'" || c === '"') {
      const start = i;
      const startLine = line;
      let value = "";
      i++;
      while (i < n && source[i] !== c && source[i] !== "\n") {
        if (source[i] === "\\") {
          value += source[i + 1] ?? "";
          i += 2;
          continue;
        }
        value += source[i];
        i++;
      }
      i++;
      tokens.push({
        type: "string",
        value,
        line: startLine,
        start,
        end: i,
        static: true,
      });
      continue;
    }
    if (c === "`") {
      const start = i;
      i++;
      readTemplate(start, line, true);
      continue;
    }
    if (c === "/" && regexAllowed()) {
      i++;
      let inClass = false;
      while (i < n && source[i] !== "\n") {
        const r = source[i];
        if (r === "\\") {
          i += 2;
          continue;
        }
        if (r === "[") inClass = true;
        else if (r === "]") inClass = false;
        else if (r === "/" && !inClass) break;
        i++;
      }
      i++;
      while (i < n && isIdentPart(source[i])) i++;
      // A regex is an operand: record it as a non-static string so that a
      // following `/` reads as division.
      tokens.push({
        type: "string",
        value: "",
        line,
        start: i,
        end: i,
        static: false,
      });
      continue;
    }
    if (isIdentStart(c)) {
      const start = i;
      while (i < n && isIdentPart(source[i])) i++;
      tokens.push({
        type: "ident",
        value: source.slice(start, i),
        line,
        start,
        end: i,
      });
      continue;
    }
    if (/[0-9]/.test(c)) {
      const start = i;
      while (i < n && /[0-9A-Za-z_.]/.test(source[i])) i++;
      tokens.push({
        type: "ident",
        value: source.slice(start, i),
        line,
        start,
        end: i,
      });
      continue;
    }
    if (c === "{") {
      braces.push(false);
    } else if (c === "}") {
      if (braces.pop()) {
        const start = i;
        i++;
        readTemplate(start, line, false);
        continue;
      }
    }
    tokens.push({ type: "punct", value: c, line, start: i, end: i + 1 });
    i++;
  }
  return tokens;
}

function isPunct(token: Token | undefined, value: string): boolean {
  return token?.type === "punct" && token.value === value;
}

function isIdent(token: Token | undefined, value: string): boolean {
  return token?.type === "ident" && token.value === value;
}

function isStaticString(
  token: Token | undefined,
): token is Extract<Token, { type: "string" }> {
  return token?.type === "string" && token.static;
}

/**
 * Finds `from "x"` at or after `start`, before the statement ends. Returns
 * the specifier token index, or -1.
 */
function findFrom(tokens: Token[], start: number): number {
  let depth = 0;
  for (let j = start; j < tokens.length; j++) {
    const t = tokens[j];
    if (t.type === "punct") {
      if (t.value === "{") depth++;
      else if (t.value === "}") depth--;
      else if (t.value === ";" && depth === 0) return -1;
      if (depth < 0) return -1;
      continue;
    }
    if (depth === 0 && isIdent(t, "from") && isStaticString(tokens[j + 1])) {
      return j + 1;
    }
    if (depth === 0 && t.type === "string") return -1;
    // Without semicolons, the next statement may itself be an import.
    if (depth === 0 && (isIdent(t, "import") || isIdent(t, "export"))) {
      return -1;
    }
  }
  return -1;
}

/**
 * Lists the module specifiers a TypeScript / JavaScript source imports.
 *
 * Covers `import … from`, `export … from`, bare `import "x"` and
 * `import("x")`. A dynamic import whose argument is not a plain literal is
 * returned with `literal: false` and its expression text as `specifier`.
 */
export function extractImports(source: string): ExtractedImport[] {
  const tokens = tokenize(source);
  const found: ExtractedImport[] = [];

  for (let i = 0; i < tokens.length; i++) {
    const t = tokens[i];
    if (t.type !== "ident") continue;
    if (t.value !== "import" && t.value !== "export") continue;
    // `x.import(...)` is a property, not the import keyword.
    if (isPunct(tokens[i - 1], ".")) continue;
    const next = tokens[i + 1];

    if (t.value === "import") {
      if (isPunct(next, ".")) continue;
      if (isPunct(next, "(")) {
        const arg = tokens[i + 2];
        const after = tokens[i + 3];
        if (
          isStaticString(arg) &&
          (isPunct(after, ")") || isPunct(after, ","))
        ) {
          found.push({
            specifier: arg.value,
            line: arg.line,
            kind: "dynamic",
            typeOnly: false,
            literal: true,
          });
        } else {
          let depth = 1;
          let j = i + 2;
          for (; j < tokens.length; j++) {
            if (isPunct(tokens[j], "(")) depth++;
            else if (isPunct(tokens[j], ")") && --depth === 0) break;
          }
          const end = tokens[j]?.start ?? source.length;
          found.push({
            specifier: source.slice(tokens[i + 2]?.start ?? end, end).trim(),
            line: t.line,
            kind: "dynamic",
            typeOnly: false,
            literal: false,
          });
        }
        continue;
      }
      if (isStaticString(next)) {
        found.push({
          specifier: next.value,
          line: next.line,
          kind: "side-effect",
          typeOnly: false,
          literal: true,
        });
        continue;
      }
      // `import type from "x"` imports a default binding named `type`.
      const typeOnly =
        isIdent(next, "type") &&
        !(isIdent(tokens[i + 2], "from") && isStaticString(tokens[i + 3]));
      const at = findFrom(tokens, i + 1);
      if (at >= 0) {
        const spec = tokens[at] as Extract<Token, { type: "string" }>;
        found.push({
          specifier: spec.value,
          line: spec.line,
          kind: "import",
          typeOnly,
          literal: true,
        });
        i = at;
      }
      continue;
    }

    const typeOnly = isIdent(next, "type");
    const head = typeOnly ? tokens[i + 2] : next;
    if (!isPunct(head, "*") && !isPunct(head, "{")) continue;
    const at = findFrom(tokens, typeOnly ? i + 2 : i + 1);
    if (at >= 0) {
      const spec = tokens[at] as Extract<Token, { type: "string" }>;
      found.push({
        specifier: spec.value,
        line: spec.line,
        kind: "re-export",
        typeOnly,
        literal: true,
      });
      i = at;
    }
  }
  return found;
}

/** Whether `specifier` is covered by one of the `allow` entries. */
export function isAllowedSpecifier(
  specifier: string,
  allow: readonly string[],
): boolean {
  return allow.some((entry) => {
    if (entry.endsWith(":") || entry.endsWith("/")) {
      return specifier.startsWith(entry);
    }
    return (
      specifier === entry ||
      specifier.startsWith(`${entry}/`) ||
      specifier.startsWith(`${entry}@`)
    );
  });
}

function isRelative(specifier: string): boolean {
  return (
    specifier.startsWith("./") ||
    specifier.startsWith("../") ||
    specifier.startsWith("/")
  );
}

async function isFile(candidate: string): Promise<boolean> {
  try {
    return (await fs.stat(candidate)).isFile();
  } catch {
    return false;
  }
}

const RESOLVABLE_EXTENSIONS = [".ts", ".tsx", ".mts", ".js", ".mjs"];

/**
 * Resolves a relative specifier the way the runtimes do for a migration:
 * the exact file, then with a known extension, then a `.js` written for a
 * `.ts` source, then a directory index.
 */
async function resolveRelative(
  fromFile: string,
  specifier: string,
): Promise<string | undefined> {
  const base = path.resolve(path.dirname(fromFile), specifier);
  const candidates = [base, ...RESOLVABLE_EXTENSIONS.map((ext) => base + ext)];
  if (/\.m?js$/.test(base)) candidates.push(base.replace(/\.(m?)js$/, ".$1ts"));
  candidates.push(
    ...RESOLVABLE_EXTENSIONS.map((ext) => path.join(base, `index${ext}`)),
  );
  for (const candidate of candidates) {
    if (await isFile(candidate)) return candidate;
  }
  return undefined;
}

function isInside(dir: string, file: string): boolean {
  const relative = path.relative(dir, file);
  return (
    relative !== "" && !relative.startsWith("..") && !path.isAbsolute(relative)
  );
}

/**
 * Scans the migration files and every relative file they reach for imports
 * that break the freeze.
 *
 * @param migrationsDir - The configured migrations directory
 * @param entryFiles - Migration files, relative to `migrationsDir` or absolute
 */
export async function scanFrozenImports(
  migrationsDir: string,
  entryFiles: readonly string[],
  policy: Pick<FrozenImportsPolicy, "allow"> & { level: "warn" | "error" },
): Promise<FrozenImportOffender[]> {
  const root = path.resolve(migrationsDir);
  const offenders: FrozenImportOffender[] = [];
  const seen = new Set<string>();
  const queue = entryFiles.map((file) => path.resolve(root, file));
  const label = (file: string) =>
    path.relative(root, file).split(path.sep).join("/");

  while (queue.length > 0) {
    const file = queue.shift()!;
    if (seen.has(file)) continue;
    seen.add(file);
    const source = await fs.readFile(file, "utf8");

    for (const found of extractImports(source)) {
      const report = (reason: string) =>
        offenders.push({
          file: label(file),
          line: found.line,
          specifier: found.specifier,
          reason,
          severity:
            found.typeOnly || policy.level === "warn" ? "warning" : "error",
        });
      const typeNote = found.typeOnly
        ? " (type-only: erased at runtime, but its types drift with the application)"
        : "";

      if (!found.literal) {
        report("dynamic import of a computed specifier cannot be verified");
        continue;
      }
      if (isRelative(found.specifier)) {
        const resolved = await resolveRelative(file, found.specifier);
        if (!resolved) {
          report(`relative import does not resolve to a file${typeNote}`);
        } else if (!isInside(root, resolved)) {
          report(`relative import leaves the migrations directory${typeNote}`);
        } else {
          queue.push(resolved);
        }
        continue;
      }
      if (!isAllowedSpecifier(found.specifier, policy.allow)) {
        report(
          `imports a module that keeps evolving after the migration ships${typeNote}`,
        );
      }
    }
  }

  return offenders.sort(
    (a, b) => a.file.localeCompare(b.file) || a.line - b.line,
  );
}
