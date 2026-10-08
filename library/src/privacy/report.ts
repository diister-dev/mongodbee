import type { PrivacyTreatment } from "./metadata.ts";
import type {
  PrivacyFinding,
  PrivacyPath,
  PrivacyPlan,
  PrivacyTarget,
} from "./plan.ts";

function pad(value: string, width: number): string {
  return value.length >= width
    ? value
    : value + " ".repeat(width - value.length);
}

function ownerLine(target: PrivacyTarget): string {
  const o = target.owner;
  switch (o.kind) {
    case "self":
      return target.delegatesTo.length > 0
        ? `person, delegates to ${target.delegatesTo.join(" | ")}`
        : "person";
    case "exempt":
      return `not personal (${o.reason ?? ""}${o.strictKeep ? ", strict: keep" : ""})`;
    case "none":
      return "no owner";
    case "ambiguous":
      return "owner AMBIGUOUS";
    default: {
      const chain =
        o.chain.length > 1 ? ` → ${o.chain.slice(1).join(" → ")}` : "";
      return `owner ${o.spaces.join(" | ")} (${o.kind} via ${o.via.join(
        ", ",
      )})${chain}`;
    }
  }
}

function pathLine(p: PrivacyPath): string {
  const role =
    p.role === "reference" && p.relation
      ? `${p.relation} → ${p.spaces.join("|")}`
      : p.role;
  const extras: string[] = [];
  if (p.mirrorOf) {
    extras.push(
      `mirror of ${p.mirrorOf}${p.normalize ? ` (${p.normalize})` : ""}`,
    );
  }
  if (p.space) extras.push(`space ${p.space}`);
  if (p.values) extras.push(`[${p.values.join(", ")}]`);
  if (p.note) extras.push(p.note);
  return `  ${pad(p.path, 34)} ${pad(role, 30)} ${pad(
    p.tier.toUpperCase() === "UNKNOWN" ? "UNKNOWN" : p.tier,
    9,
  )} ${pad(p.treatment.extract, 11)} ${extras.join(" · ")}`.trimEnd();
}

function findingLine(f: PrivacyFinding): string {
  const where = f.path ? `${f.target} ${f.path}` : f.target;
  return `  ${pad(f.level, 8)} ${where}\n           ${f.message}`;
}

const CLEAR_TREATMENTS: ReadonlySet<PrivacyTreatment> = new Set([
  "keep",
  "include",
]);

function clearLine(p: PrivacyPath): string {
  const why =
    p.role === "none"
      ? (p.note ?? "no personal signal")
      : `${p.role}${p.note ? ` · ${p.note}` : ""}`;
  return `  ${pad(p.path, 34)} ${pad(p.tier, 9)} ${why}`;
}

function keptInClear(plan: PrivacyPlan): string[] {
  const lines: string[] = ["kept in clear on extract"];
  let count = 0;
  for (const target of plan.targets.values()) {
    const kept = target.paths.filter(
      (p) =>
        p.role !== "reference" && CLEAR_TREATMENTS.has(p.treatment.extract),
    );
    if (kept.length === 0) continue;
    count += kept.length;
    lines.push(`  ${target.key}`);
    for (const p of kept) lines.push(`  ${clearLine(p)}`);
  }
  if (count === 0) {
    lines.push("  nothing: every value is remapped, replaced or dropped");
  }
  return lines;
}

export function renderPrivacyReport(plan: PrivacyPlan): string {
  const lines: string[] = [];
  if (plan.posture === "strict") {
    lines.push(
      "posture     strict: undeclared values are faked, not kept or dropped",
      "            untyped payloads keep their shape: ids remapped, dates shifted, strings (and numbers of person documents) faked, binary values dropped",
      "",
    );
  }

  lines.push("persons");
  if (plan.persons.size === 0) {
    lines.push("  NONE DECLARED: nothing below can be personal, see findings");
  }
  for (const person of plan.persons.values()) {
    const delegation =
      person.delegatesTo.length > 0
        ? `delegates to ${person.delegatesTo.join(" | ")}`
        : "root";
    lines.push(
      `  ${pad(person.space, 34)} ${pad(person.target, 50)} ${delegation}`,
    );
  }
  lines.push("");

  for (const target of plan.targets.values()) {
    lines.push(
      `${target.key}    ${target.paths.length} paths · ${ownerLine(target)}`,
    );
    for (const p of target.paths) {
      if (p.tier === "none") continue;
      lines.push(pathLine(p));
    }
    lines.push("");
  }

  const errors = plan.findings.filter((f) => f.level === "error");
  const warnings = plan.findings.filter((f) => f.level === "warning");
  const infos = plan.findings.filter((f) => f.level === "info");
  if (plan.findings.length > 0) {
    lines.push(
      `findings    ${errors.length} error · ${warnings.length} warning · ${infos.length} info`,
    );
    for (const f of [...errors, ...warnings, ...infos]) {
      lines.push(findingLine(f));
    }
    lines.push("");
  }

  lines.push(...keptInClear(plan), "");

  const s = plan.summary;
  lines.push(
    `summary     ${s.certain} certain · ${s.inferred} inferred · ${s.declared} declared · ${s.dynamic} dynamic · ${s.unknown} UNKNOWN · ${s.none} none${plan.posture === "strict" ? ` · ${s.faked} replaced by the strict posture` : ""}`,
  );
  return lines.join("\n");
}
