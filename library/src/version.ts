/**
 * The package's own identity, as a plain module.
 *
 * Kept in step with `package.json` by `bun run version:set <version>`, and
 * verified against it by `bun run check:package` (which runs as `prebuild`), so
 * the three places the version lives cannot drift apart unnoticed.
 *
 * This exists because the manifest cannot be imported at runtime: a JSON import
 * resolved as `../package.json` is correct in the source tree but wrong in the
 * build output, where the file sits one directory deeper, and the two published
 * manifests (`package.json` for npm, `jsr.json` for JSR) would each need their
 * own import path. Generating a module sidesteps both.
 *
 * @module
 */

/** The package name, identical on npm and JSR. */
export const NAME = "@diister/mongodbee";

/** The package version, mirroring `package.json`. */
export const VERSION = "0.23.0-beta.20";
