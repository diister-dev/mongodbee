<script lang="ts">
  import Absent from "./Absent.svelte";
  import ArrayValue from "./ArrayValue.svelte";
  import Bool from "./Bool.svelte";
  import DateValue from "./DateValue.svelte";
  import Email from "./Email.svelte";
  import IdChunks from "./IdChunks.svelte";
  import Enum from "./Enum.svelte";
  import Null from "./Null.svelte";
  import Num from "./Num.svelte";
  import ObjectId from "./ObjectId.svelte";
  import ObjectToken from "./ObjectToken.svelte";
  import RefId from "./RefId.svelte";
  import Scope from "./Scope.svelte";
  import Text from "./Text.svelte";
  import TypeTag from "./TypeTag.svelte";
  import Ulid from "./Ulid.svelte";
  import Url from "./Url.svelte";
  import Variant from "./Variant.svelte";
  import { isEmail, isObjectIdHex, isUlid, isUrl, parseTypedId } from "../lib/values.ts";

  interface SchemaNode {
    kind?: string;
    ref?: string;
    values?: unknown[];
    item?: SchemaNode;
    [key: string]: unknown;
  }

  interface Props {
    value: unknown;
    node?: SchemaNode;
    field?: string;
    onopen?: () => void;
    compact?: boolean;
    json?: boolean;
    reveal?: boolean;
  }

  let { value, node, field, onopen, compact = false, json = false, reveal = false }: Props = $props();

  function wrapper(v: unknown): string | null {
    if (!v || typeof v !== "object" || Array.isArray(v)) return null;
    const keys = Object.keys(v);
    return keys.length === 1 && keys[0].startsWith("$") ? keys[0] : null;
  }

  const discriminator = $derived.by((): string | undefined => {
    const v = value as Record<string, unknown> | null;
    if (!v || typeof v !== "object" || Array.isArray(v)) return undefined;
    if (node?.kind === "variant" && typeof node.discriminator === "string") {
      return typeof v[node.discriminator] === "string" ? node.discriminator : undefined;
    }
    if (node) return undefined;
    for (const key of ["type", "kind"]) {
      if (typeof v[key] === "string" && Object.keys(v).length <= 6) return key;
    }
    return undefined;
  });

  const kind = $derived.by((): string => {
    const v = value;
    if (v === undefined) return "absent";
    if (v === null) return "null";
    if (field === "_type" && typeof v === "string") return "type";
    if (field === "_scope") return "scope";
    const w = wrapper(v);
    if (w === "$date") return "date";
    if (w === "$oid") return "oid";
    if (w === "$numberDecimal" || w === "$numberLong" || w === "$numberInt" || w === "$numberDouble") return "numeric";
    if (node?.ref && typeof v === "string") return "ref";
    if ((node?.kind === "picklist" || node?.kind === "enum") && (typeof v === "string" || typeof v === "number")) {
      return "enum";
    }
    if (typeof v === "boolean") return "bool";
    if (typeof v === "number") return "number";
    if (Array.isArray(v)) return "array";
    if (typeof v === "object") {
      if (discriminator) return "variant";
      return "object";
    }
    if (typeof v === "string") {
      if (node?.kind === "date") return "date";
      if (parseTypedId(v) && !isUrl(v)) return "ref";
      if (isUlid(v)) return "ulid";
      if (isObjectIdHex(v)) return "oidHex";
      if (isEmail(v)) return "email";
      if (isUrl(v)) return "url";
      if (field === "_id") return "id";
      return "text";
    }
    return "text";
  });

  const wrapped = $derived((value as Record<string, unknown>) ?? {});
</script>

{#if kind === "absent"}
  <Absent {field} />
{:else if kind === "null"}
  <Null />
{:else if kind === "type"}
  <TypeTag name={value as string} />
{:else if kind === "scope"}
  <Scope {value} />
{:else if kind === "date"}
  <DateValue {value} {reveal} />
{:else if kind === "oid"}
  <ObjectId value={String(wrapped.$oid)} {reveal} />
{:else if kind === "oidHex"}
  <ObjectId value={value as string} {reveal} />
{:else if kind === "numeric"}
  <Num value={String(Object.values(wrapped)[0])} />
{:else if kind === "ref"}
  <RefId value={value as string} target={node?.ref} self={field === "_id"} {reveal} />
{:else if kind === "enum"}
  <Enum value={value as string} options={node?.values} {field} />
{:else if kind === "bool"}
  <Bool value={value as boolean} />
{:else if kind === "number"}
  <Num value={value as number} />
{:else if kind === "array"}
  <ArrayValue value={value as unknown[]} item={node?.item} {field} visible={compact ? 1 : 2} />
{:else if kind === "variant"}
  <Variant
    value={value as Record<string, unknown>}
    discriminator={discriminator ?? "type"}
    node={node as never}
    {field}
    {onopen}
  />
{:else if kind === "object"}
  <ObjectToken value={value as Record<string, unknown>} {onopen} />
{:else if kind === "ulid"}
  <Ulid value={value as string} {reveal} />
{:else if kind === "email"}
  <Email value={value as string} />
{:else if kind === "url"}
  <Url value={value as string} />
{:else if kind === "id"}
  <IdChunks value={value as string} />
{:else}
  <Text value={String(value)} quoted={json} />
{/if}
