import process from "node:process";
import { createHash } from "node:crypto";
import * as v from "../src/schema.ts";
import { dbId, refId } from "../src/ids.ts";
import { migrationDefinition } from "../src/migration/definition.ts";
import { createSimulationValidator } from "../src/migration/validators/simulation.ts";
import {
  createEmptyDatabaseState,
  type MigrationDefinition,
  type SchemasDefinition,
  type SimulationDatabaseState,
} from "../src/migration/types.ts";
import type { SimulationPowerLevel } from "../src/migration/validators/mock/config.ts";

const MIGRATIONS = Number(process.env.BENCH_MIGRATIONS ?? "24");
const MODES = (process.env.BENCH_MODES ?? "quick,normal").split(
  ",",
) as SimulationPowerLevel[];
const MEMORY_SAMPLE_MS = 50;

type Doc = Record<string, unknown>;
type Fields = Record<string, v.GenericSchema>;

const slug = v.pipe(v.string(), v.regex(/^[a-z0-9]+(?:-[a-z0-9]+)*$/));
const ulidLike = v.pipe(v.string(), v.regex(/^[0-9a-z]{26}$/));
const hexColor = v.pipe(v.string(), v.hexColor());

const baseCollections = {
  "+expositions": {
    _id: dbId("exposition"),
    name: v.string(),
    slug,
    organizationId: refId("organization"),
    startsAt: v.date(),
  },
  "+organizations": {
    _id: dbId("organization"),
    name: v.string(),
    slug,
    color: hexColor,
    owner: refId("user"),
  },
  "+users": {
    _id: dbId("user"),
    email: v.pipe(v.string(), v.email()),
    firstname: v.string(),
    lastname: v.string(),
    organizationIds: v.array(refId("organization")),
    token: ulidLike,
    address: v.object({
      city: v.string(),
      zip: v.pipe(v.string(), v.regex(/^[0-9]{5}$/)),
    }),
  },
};

const catalogTypes = {
  product: {
    _id: dbId("product"),
    slug,
    price: v.pipe(v.number(), v.integer(), v.minValue(0)),
    organizationId: refId("organization"),
  },
  category: {
    _id: dbId("category"),
    label: v.string(),
    parentId: v.nullable(refId("category")),
  },
};

const eventTypes = {
  session: {
    _id: dbId("session"),
    title: v.string(),
    slug,
    speakerIds: v.array(refId("user")),
    startsAt: v.date(),
  },
  registration: {
    _id: dbId("registration"),
    userId: refId("user"),
    sessionId: refId("session"),
    status: v.picklist(["confirmed", "waitlisted", "cancelled"]),
    code: v.pipe(v.string(), v.regex(/^[A-Z]{3}-[0-9]{4}$/)),
  },
};

const expositionModel = {
  information: {
    _id: v.literal("information:0"),
    name: v.string(),
    slug,
    organizationId: refId("organization"),
  },
  participant: {
    _id: dbId("participant"),
    userId: refId("user"),
    badge: v.pipe(v.string(), v.regex(/^[A-Z0-9]{8}$/)),
    email: v.pipe(v.string(), v.email()),
  },
};

function addedCollection(index: number): Fields {
  return {
    _id: dbId(`record${index}`),
    slug,
    reference: v.pipe(v.string(), v.regex(/^REF-[0-9]{6}-[A-Z0-9]{4}$/)),
    userId: refId("user"),
    organizationId: v.optional(refId("organization")),
    tags: v.array(slug),
    meta: v.object({
      version: v.pipe(v.number(), v.integer()),
      checksum: v.pipe(v.string(), v.regex(/^[0-9a-f]{40}$/)),
      notes: v.nullable(v.string()),
    }),
  };
}

const withField = (fields: Fields, key: string, schema: v.GenericSchema) => ({
  ...fields,
  [key]: schema,
});

const omit = (doc: Doc, key: string): Doc => {
  const { [key]: _dropped, ...rest } = doc;
  return rest;
};

const stepField = (index: number) => `step${index}`;
const stepValue = (index: number) => `value-${index}`;

/**
 * A chain shaped like a real one: an init declaring every bucket kind, then
 * migrations that alternately add a regex-heavy collection and reshape the
 * existing ones with reversible transforms, so the state grows along the chain.
 */
function buildChain(length: number): MigrationDefinition[] {
  const chain: MigrationDefinition[] = [];
  let schemas: SchemasDefinition = {
    collections: { ...baseCollections },
    multiCollections: { "+catalog": { ...catalogTypes } },
    scopedMultiCollections: {
      "+events": { scope: refId("exposition"), types: { ...eventTypes } },
    },
    multiModels: { exposition: { ...expositionModel } },
  };
  const init = migrationDefinition("0000", "init", {
    parent: null,
    schemas,
    migrate: (b) => {
      b.createCollection("+expositions");
      b.createCollection("+organizations");
      b.createCollection("+users");
      b.createMultiCollection("+catalog");
      b.createScopedMultiCollection("+events");
      return b.compile();
    },
  });
  chain.push(init);

  for (let index = 1; index < length; index++) {
    const parent = chain[chain.length - 1]!;
    const previous = schemas;
    const id = String(index).padStart(4, "0");
    const field = stepField(index);
    const value = stepValue(index);
    const up = (doc: Doc) => ({ ...doc, [field]: value });
    const down = (doc: Doc) => omit(doc, field);
    const fieldSchema = v.literal(value);
    let migration: MigrationDefinition;

    switch (index % 5) {
      case 1: {
        const name = `+records${index}`;
        schemas = {
          ...previous,
          collections: {
            ...previous.collections,
            [name]: addedCollection(index),
          },
        };
        migration = migrationDefinition(id, `add_records${index}`, {
          parent,
          schemas,
          migrate: (b) => {
            b.createCollection(name);
            return b.compile();
          },
        });
        break;
      }
      case 2: {
        const users = previous.collections!["+users"] as Fields;
        schemas = {
          ...previous,
          collections: {
            ...previous.collections,
            "+users": withField(users, field, fieldSchema),
          },
        };
        migration = migrationDefinition(id, `users_${field}`, {
          parent,
          schemas,
          migrate: (b) => {
            b.collection("+users").transform({ up, down });
            return b.compile();
          },
        });
        break;
      }
      case 3: {
        const catalog = previous.multiCollections!["+catalog"]!;
        schemas = {
          ...previous,
          multiCollections: {
            ...previous.multiCollections,
            "+catalog": {
              ...catalog,
              product: withField(catalog.product as Fields, field, fieldSchema),
            },
          },
        };
        migration = migrationDefinition(id, `catalog_${field}`, {
          parent,
          schemas,
          migrate: (b) =>
            b
              .multiCollection("+catalog")
              .type("product")
              .transform({ up, down })
              .end()
              .end()
              .compile(),
        });
        break;
      }
      case 4: {
        const model = previous.multiModels!.exposition!;
        schemas = {
          ...previous,
          multiModels: {
            ...previous.multiModels,
            exposition: {
              ...model,
              participant: withField(
                model.participant as Fields,
                field,
                fieldSchema,
              ),
            },
          },
        };
        migration = migrationDefinition(id, `participants_${field}`, {
          parent,
          schemas,
          migrate: (b) =>
            b
              .multiModelInstances("exposition")
              .type("participant")
              .transform({ up, down })
              .end()
              .end()
              .compile(),
        });
        break;
      }
      default: {
        const events = previous.scopedMultiCollections!["+events"]!;
        schemas = {
          ...previous,
          scopedMultiCollections: {
            ...previous.scopedMultiCollections,
            "+events": {
              ...events,
              types: {
                ...events.types,
                session: withField(
                  events.types.session as Fields,
                  field,
                  fieldSchema,
                ),
              },
            },
          },
        };
        migration = migrationDefinition(id, `events_${field}`, {
          parent,
          schemas,
          migrate: (b) =>
            b
              .scopedMultiCollection("+events")
              .type("session")
              .transform({ up, down })
              .end()
              .end()
              .compile(),
        });
      }
    }
    chain.push(migration);
  }
  return chain;
}

type Peak = { rss: number; heapUsed: number };

function trackMemory(): { stop: () => Peak } {
  const peak: Peak = { rss: 0, heapUsed: 0 };
  const sample = () => {
    const usage = process.memoryUsage();
    peak.rss = Math.max(peak.rss, usage.rss);
    peak.heapUsed = Math.max(peak.heapUsed, usage.heapUsed);
  };
  sample();
  const timer = setInterval(sample, MEMORY_SAMPLE_MS);
  return {
    stop: () => {
      clearInterval(timer);
      sample();
      return peak;
    },
  };
}

/** Runs the chain the way `check` does: validate, then propagate the state. */
async function runChain(
  chain: MigrationDefinition[],
  powerLevel: SimulationPowerLevel,
) {
  // The validator's hot loops never yield, so the memory sampler only runs
  // when the progress callback lets it — once every few thousand notes.
  let notes = 0;
  const validator = createSimulationValidator({
    powerLevel,
    onProgress: () => {
      if (++notes % 2000 === 0) {
        const usage = process.memoryUsage();
        peakFromProgress.rss = Math.max(peakFromProgress.rss, usage.rss);
        peakFromProgress.heapUsed = Math.max(
          peakFromProgress.heapUsed,
          usage.heapUsed,
        );
      }
    },
  });
  const peakFromProgress: Peak = { rss: 0, heapUsed: 0 };
  const verdicts: Array<{ id: string; errors: string[]; warnings: string[] }> =
    [];
  let state: SimulationDatabaseState = createEmptyDatabaseState();
  const memory = trackMemory();
  const start = performance.now();
  for (const migration of chain) {
    const result = await validator.validateMigration(migration, state);
    verdicts.push({
      id: migration.id,
      errors: result.errors,
      warnings: result.warnings,
    });
    if (process.env.BENCH_VERBOSE) {
      for (const line of [...result.errors, ...result.warnings]) {
        console.log(`${migration.id} ${line}`);
      }
    }
    const after = result.data?.stateAfterMigration as
      | SimulationDatabaseState
      | undefined;
    if (!result.success || !after) break;
    state = validator.prepareStateForNextMigration(after, migration.schemas);
  }
  const elapsed = performance.now() - start;
  const sampled = memory.stop();
  return {
    elapsed,
    peak: {
      rss: Math.max(sampled.rss, peakFromProgress.rss),
      heapUsed: Math.max(sampled.heapUsed, peakFromProgress.heapUsed),
    },
    verdicts,
  };
}

const chain = buildChain(MIGRATIONS);
const mb = (bytes: number) => `${Math.round(bytes / 1024 / 1024)} MB`;

for (const mode of MODES) {
  const { elapsed, peak, verdicts } = await runChain(chain, mode);
  const errors = verdicts.reduce((n, entry) => n + entry.errors.length, 0);
  const warnings = verdicts.reduce((n, entry) => n + entry.warnings.length, 0);
  const digest = createHash("sha256")
    .update(JSON.stringify(verdicts))
    .digest("hex")
    .slice(0, 16);
  console.log(
    `${`simulation ${mode} (${verdicts.length}/${chain.length} migrations)`.padEnd(40)} ${String(Math.round(elapsed)).padStart(8)} ms   peak rss ${mb(peak.rss).padStart(8)}   peak heap ${mb(peak.heapUsed).padStart(8)}   ${errors} errors ${warnings} warnings   verdicts ${digest}`,
  );
  if (process.env.BENCH_JSON) {
    console.log(
      `JSON ${JSON.stringify({ mode, elapsed, peak, errors, warnings, digest })}`,
    );
  }
}
