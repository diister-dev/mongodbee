<script>
  import CommandChip from "./CommandChip.svelte";
  import EmptyState from "./EmptyState.svelte";
  import ErrorState from "./ErrorState.svelte";
  import { ApiError } from "./lib/api.js";
  import { SearchX } from "./lib/icons.js";
  import Status from "./Status.svelte";
  import Spectrum from "./controls/Spectrum.svelte";
  import Absent from "./values/Absent.svelte";
  import ArrayValue from "./values/ArrayValue.svelte";
  import Bool from "./values/Bool.svelte";
  import DateValue from "./values/DateValue.svelte";
  import Email from "./values/Email.svelte";
  import Enum from "./values/Enum.svelte";
  import IndexKey from "./values/IndexKey.svelte";
  import MigrationId from "./values/MigrationId.svelte";
  import Null from "./values/Null.svelte";
  import Duration from "./values/Duration.svelte";
  import Num from "./values/Num.svelte";
  import ObjectId from "./values/ObjectId.svelte";
  import ObjectToken from "./values/ObjectToken.svelte";
  import RefId from "./values/RefId.svelte";
  import Scope from "./values/Scope.svelte";
  import TypeTag from "./values/TypeTag.svelte";
  import Ulid from "./values/Ulid.svelte";
  import Url from "./values/Url.svelte";

  const now = Date.now();
  const STATUS = ["draft", "review", "published", "archived"];
</script>

<div class="page">
  <header class="head">
    <span class="eyebrow">gallery</span>
    <h1 class="page-title">Value components</h1>
    <p class="muted">One component per data concept, as used in tables, the document drawer, schemas, indexes and migrations.</p>
  </header>

  <div class="grid">
    <section class="bezel">
      <div class="bezel-card body">
        <h2 class="section-title">Brand</h2>
        <div class="row"><span class="label">spectrum mark</span><Spectrum size={8} gap={2} label="mongodbee spectrum" /></div>
        <div class="row"><span class="label">loading</span><Spectrum size={8} gap={2} loading /></div>
        <div class="row"><span class="label">eyebrow</span><span class="eyebrow">migration chain</span></div>
        <div class="row"><span class="label">command</span><CommandChip command="mongodbee migrate" /></div>
      </div>
      <footer class="bezel-foot"><span class="file mono">Spectrum.svelte, CommandChip.svelte</span></footer>
    </section>

    <section class="bezel">
      <div class="bezel-card body">
        <h2 class="section-title">Status</h2>
        <div class="row"><span class="label">success</span><Status tone="success" label="applied" /></div>
        <div class="row"><span class="label">pending</span><Status tone="warning" hollow label="pending" /></div>
        <div class="row"><span class="label">failure</span><Status tone="danger" label="failed" /></div>
        <div class="row"><span class="label">neutral, live</span><span class="inline"><Status tone="neutral" label="extra" /><Status tone="live" label="read-only" /></span></div>
      </div>
      <footer class="bezel-foot"><span class="file mono">Status.svelte</span></footer>
    </section>

    <section class="bezel">
      <div class="bezel-card body">
        <h2 class="section-title">Reference</h2>
        <div class="row"><span class="label">ulid, browsable</span><RefId value="artwork:01j9zk3v4q8m2c7xw5t6yhnbre" /></div>
        <div class="row"><span class="label">seed id</span><RefId value="category:books" /></div>
        <div class="row"><span class="label">unknown type</span><RefId value="artist:a4" target="artist" /></div>
        <div class="row"><span class="label">document itself</span><RefId value="visit:01j9zm0c2ae7h3r1b8k5n4d6fq" self /></div>
      </div>
      <footer class="bezel-foot"><span class="file mono">RefId.svelte</span></footer>
    </section>

    <section class="bezel">
      <div class="bezel-card body">
        <h2 class="section-title">ULID and ObjectId</h2>
        <div class="row"><span class="label">ulid</span><Ulid value="01j9zk3v4q8m2c7xw5t6yhnbre" /></div>
        <div class="row"><span class="label">ulid, upper</span><Ulid value="01ARZ3NDEKTSV4RRFFQ69G5FAV" /></div>
        <div class="row"><span class="label">objectid</span><ObjectId value="6ab9711ce43f4ca84383bee1" /></div>
      </div>
      <footer class="bezel-foot"><span class="file mono">Ulid.svelte, ObjectId.svelte</span></footer>
    </section>

    <section class="bezel">
      <div class="bezel-card body">
        <h2 class="section-title">Scope and type</h2>
        <div class="row"><span class="label">scope</span><Scope value="exposition:beta02" interactive={false} /></div>
        <div class="row"><span class="label">scope, long</span><Scope value="exposition:01j9zk3v4q8m2c7xw5t6yhnbre" interactive={false} /></div>
        <div class="row">
          <span class="label">types</span>
          <span class="inline">
            {#each ["artwork", "visit", "product", "category", "note", "tag"] as name}<TypeTag {name} />{/each}
          </span>
        </div>
      </div>
      <footer class="bezel-foot"><span class="file mono">Scope.svelte, TypeTag.svelte</span></footer>
    </section>

    <section class="bezel">
      <div class="bezel-card body">
        <h2 class="section-title">Picklist</h2>
        {#each STATUS as value}
          <div class="row"><span class="label">status</span><Enum {value} options={STATUS} field="status" /></div>
        {/each}
      </div>
      <footer class="bezel-foot"><span class="file mono">Enum.svelte</span></footer>
    </section>

    <section class="bezel">
      <div class="bezel-card body">
        <h2 class="section-title">Dates</h2>
        <div class="row"><span class="label">day</span><DateValue value={{ $date: "2026-04-22T00:00:00Z" }} /></div>
        <div class="row"><span class="label">with time</span><DateValue value={{ $date: "2026-09-27T20:03:29Z" }} /></div>
        <div class="row"><span class="label">future</span><DateValue value={now + 3 * 24 * 3600 * 1000 + 9 * 60 * 1000} /></div>
      </div>
      <footer class="bezel-foot"><span class="file mono">DateValue.svelte</span></footer>
    </section>

    <section class="bezel">
      <div class="bezel-card body">
        <h2 class="section-title">Numbers and booleans</h2>
        <div class="row num-row"><span class="label">integer</span><Num value={1808} /></div>
        <div class="row num-row"><span class="label">decimal</span><Num value={0.1 + 0.2} /></div>
        <div class="row num-row"><span class="label">count</span><Num value={1284937} count /></div>
        <div class="row num-row"><span class="label">duration</span><span class="inline"><Duration ms={840} /><Duration ms={12343} /><Duration ms={125000} /></span></div>
        <div class="row"><span class="label">true, false</span><span class="inline"><Bool value={true} /><Bool value={false} /></span></div>
      </div>
      <footer class="bezel-foot"><span class="file mono">Num.svelte, Duration.svelte, Bool.svelte</span></footer>
    </section>

    <section class="bezel">
      <div class="bezel-card body">
        <h2 class="section-title">Email and URL</h2>
        <div class="row"><span class="label">email</span><Email value="user105@example.com" /></div>
        <div class="row"><span class="label">long email</span><Email value="first.middle.lastname.department@very-long-company-domain.example" /></div>
        <div class="row"><span class="label">url</span><Url value="https://docs.example.com/guides/getting-started/installation?ref=studio" /></div>
      </div>
      <footer class="bezel-foot"><span class="file mono">Email.svelte, Url.svelte</span></footer>
    </section>

    <section class="bezel">
      <div class="bezel-card body">
        <h2 class="section-title">Collections of values</h2>
        <div class="row"><span class="label">strings</span><ArrayValue value={["a", "b", "c", "d"]} /></div>
        <div class="row">
          <span class="label">refs</span>
          <ArrayValue value={["artwork:01j9zk3v4q8m2c7xw5t6yhnbre", "artwork:01j9zm0c2ae7h3r1b8k5n4d6fq"]} />
        </div>
        <div class="row"><span class="label">empty</span><ArrayValue value={[]} /></div>
        <div class="row"><span class="label">object</span><ObjectToken value={{ city: "Lyon", zip: null }} onopen={() => {}} /></div>
      </div>
      <footer class="bezel-foot"><span class="file mono">ArrayValue.svelte, ObjectToken.svelte</span></footer>
    </section>

    <section class="bezel">
      <div class="bezel-card body">
        <h2 class="section-title">Missing values</h2>
        <div class="row"><span class="label">null</span><Null /></div>
        <div class="row"><span class="label">absent</span><span class="absent-demo"><Absent field="address" /></span></div>
      </div>
      <footer class="bezel-foot"><span class="file mono">Null.svelte, Absent.svelte</span></footer>
    </section>

    <section class="bezel">
      <div class="bezel-card body">
        <h2 class="section-title">Index keys and migrations</h2>
        <div class="row"><span class="label">compound</span><IndexKey fields={{ role: 1, createdAt: -1 }} /></div>
        <div class="row"><span class="label">unique</span><IndexKey fields={{ email: 1 }} unique /></div>
        <div class="row">
          <span class="label">scoped</span><IndexKey fields={{ _scope: 1, _type: 1, title: 1 }} unique partial={{ _type: "artwork" }} />
        </div>
        <div class="row"><span class="label">migration</span><MigrationId value="2026_09_27_002_user_roles.ts" /></div>
      </div>
      <footer class="bezel-foot"><span class="file mono">IndexKey.svelte, MigrationId.svelte</span></footer>
    </section>

    <section class="bezel">
      <div class="bezel-card body states">
        <h2 class="section-title">Errors</h2>
        <ErrorState
          compact
          error={new ApiError("Failed to fetch", { path: "/api/overview", transport: true })}
          onretry={() => {}}
        />
        <ErrorState
          compact
          title="No document user:01k in +users"
          error={new ApiError('No document with _id "user:01k"', { status: 404, path: "/api/collections/+users/document" })}
        />
        <ErrorState
          compact
          title="The query was refused"
          error={new ApiError('"abc" is not a number', { status: 400, path: "/api/collections/+users/documents" })}
          onretry={() => {}}
        />
      </div>
      <footer class="bezel-foot"><span class="file mono">ErrorState.svelte</span></footer>
    </section>

    <section class="bezel">
      <div class="bezel-card body">
        <h2 class="section-title">Empty states</h2>
        <EmptyState icon={SearchX} title="Nothing matches" hint="No document matches the 2 conditions.">
          <button class="control" type="button">Clear conditions</button>
        </EmptyState>
      </div>
      <footer class="bezel-foot"><span class="file mono">EmptyState.svelte</span></footer>
    </section>
  </div>
</div>

<style>
  .page {
    display: flex;
    flex: 1;
    flex-direction: column;
    gap: 20px;
    min-height: 0;
    padding: 12px 0 24px 8px;
    overflow: auto;
  }

  .head {
    display: flex;
    flex-direction: column;
    gap: 4px;
  }

  .head p {
    margin: 0;
  }

  .grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(360px, 1fr));
    gap: 16px;
    align-items: start;
  }

  .body {
    gap: 0;
    padding: 14px 16px 10px;
  }

  .body.states {
    gap: 10px;
  }

  .body .section-title {
    margin-bottom: 6px;
    font-size: var(--text-sm);
  }

  .row {
    display: grid;
    grid-template-columns: 104px minmax(0, 1fr);
    align-items: center;
    justify-items: start;
    min-height: 32px;
    border-bottom: 1px solid var(--hairline);
  }

  .row:last-child {
    border-bottom: 0;
  }

  .num-row > :last-child {
    justify-self: end;
  }

  .label {
    color: var(--text-faint);
    font-size: var(--text-xs);
  }

  .inline {
    display: inline-flex;
    flex-wrap: wrap;
    gap: 6px;
  }

  .absent-demo {
    display: inline-flex;
    width: 80px;
    border: 1px dashed var(--card-border);
    border-radius: 0;
  }

  .file {
    color: var(--text-faint);
  }
</style>
