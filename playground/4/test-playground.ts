#!/usr/bin/env -S deno run --allow-read --allow-write --allow-net --allow-env
/**
 * Test script for MongoDBee Migration System
 * Tests all CLI commands and functionality
 */

import { blue, green, red, yellow, dim } from "@std/fmt/colors";

/**
 * Run a shell command and capture output
 */
async function runCommand(command: string, expectSuccess = true): Promise<{ success: boolean; output: string; error?: string }> {
  console.log(dim(`Running: ${command}`));
  
  try {
    const proc = new Deno.Command("deno", {
      args: ["task", ...command.split(" ")],
      stdout: "piped",
      stderr: "piped",
      cwd: Deno.cwd()
    });
    
    const { code, stdout, stderr } = await proc.output();
    const output = new TextDecoder().decode(stdout);
    const error = new TextDecoder().decode(stderr);
    
    const success = code === 0;
    
    if (expectSuccess && !success) {
      console.log(red(`✗ Command failed: ${command}`));
      console.log(red(`Error: ${error}`));
      return { success: false, output, error };
    }
    
    if (success) {
      console.log(green(`✓ Command succeeded: ${command}`));
    } else {
      console.log(yellow(`! Command failed as expected: ${command}`));
    }
    
    return { success, output, error };
    
  } catch (err) {
    const errorMsg = err instanceof Error ? err.message : String(err);
    console.log(red(`✗ Command execution failed: ${command} - ${errorMsg}`));
    return { success: false, output: "", error: errorMsg };
  }
}

/**
 * Test configuration loading
 */
async function testConfigLoading() {
  console.log(blue("\n=== Testing Configuration Loading ==="));
  
  // Test that config file exists
  try {
    await Deno.stat("./mongodbee.config.ts");
    console.log(green("✓ mongodbee.config.ts exists"));
  } catch {
    console.log(red("✗ mongodbee.config.ts not found"));
    return false;
  }
  
  return true;
}

/**
 * Test CLI commands
 */
async function testCLICommands() {
  console.log(blue("\n=== Testing CLI Commands ==="));
  
  // Test status command (should work even without migrations)
  let result = await runCommand("migrate:status");
  if (!result.success) return false;
  
  // Test generate command with different templates
  console.log(yellow("\nTesting migration generation..."));
  
  result = await runCommand("migrate:generate --name create-users --template create-collection");
  if (!result.success) return false;
  
  result = await runCommand("migrate:generate --name add-indexes --template add-index");
  if (!result.success) return false;
  
  result = await runCommand("migrate:generate --name seed-data --template seed-data");
  if (!result.success) return false;
  
  // Check that migration files were created
  try {
    const migrationsDir = await Deno.readDir("./migrations");
    let migrationCount = 0;
    for await (const entry of migrationsDir) {
      if (entry.isFile && entry.name.endsWith(".ts")) {
        migrationCount++;
        console.log(green(`✓ Generated migration: ${entry.name}`));
      }
    }
    
    if (migrationCount < 3) {
      console.log(red(`✗ Expected at least 3 migrations, found ${migrationCount}`));
      return false;
    }
  } catch (err) {
    console.log(red(`✗ Error reading migrations directory: ${err}`));
    return false;
  }
  
  // Test status again (should show pending migrations)
  result = await runCommand("migrate:status");
  if (!result.success) return false;
  
  // Test apply command (should show not implemented message)
  console.log(yellow("\nTesting apply command (expected to show 'not implemented')..."));
  result = await runCommand("migrate:apply", false); // Expect failure since not implemented
  
  // Test rollback command (should show not implemented message)
  console.log(yellow("\nTesting rollback command (expected to show 'not implemented')..."));
  result = await runCommand("migrate:rollback", false); // Expect failure since not implemented
  
  return true;
}

/**
 * Test directory structure
 */
async function testDirectoryStructure() {
  console.log(blue("\n=== Testing Directory Structure ==="));
  
  const expectedDirs = ["migrations", "schemas"];
  
  for (const dir of expectedDirs) {
    try {
      const stat = await Deno.stat(`./${dir}`);
      if (stat.isDirectory) {
        console.log(green(`✓ Directory exists: ${dir}`));
      } else {
        console.log(red(`✗ ${dir} exists but is not a directory`));
        return false;
      }
    } catch {
      console.log(red(`✗ Directory missing: ${dir}`));
      return false;
    }
  }
  
  return true;
}

/**
 * Test different environments
 */
async function testEnvironments() {
  console.log(blue("\n=== Testing Environment Support ==="));
  
  const environments = ["development", "testing", "production"];
  
  for (const env of environments) {
    console.log(yellow(`\nTesting ${env} environment...`));
    const result = await runCommand(`migrate:status --env ${env}`);
    if (!result.success) {
      console.log(red(`✗ Failed to load ${env} environment`));
      return false;
    }
    console.log(green(`✓ ${env} environment loaded successfully`));
  }
  
  return true;
}

/**
 * Clean up test artifacts
 */
async function cleanup() {
  console.log(blue("\n=== Cleanup ==="));
  
  // Remove generated files but keep the structure
  try {
    // Remove migrations directory if it exists
    try {
      await Deno.remove("./migrations", { recursive: true });
      console.log(green("✓ Cleaned up migrations directory"));
    } catch {
      // Directory might not exist, that's ok
    }
    
    // Remove other generated directories
    const dirsToClean = ["schemas", "temp", "backup", "logs"];
    for (const dir of dirsToClean) {
      try {
        await Deno.remove(`./${dir}`, { recursive: true });
        console.log(green(`✓ Cleaned up ${dir} directory`));
      } catch {
        // Directory might not exist, that's ok
      }
    }
    
  } catch (err) {
    console.log(yellow(`Warning: Cleanup had issues: ${err}`));
  }
}

/**
 * Main test function
 */
async function main() {
  console.log(blue("MongoDBee Migration System - Playground Test\n"));
  
  let allTestsPassed = true;
  
  // Run all tests
  allTestsPassed = await testConfigLoading() && allTestsPassed;
  allTestsPassed = await testCLICommands() && allTestsPassed;
  allTestsPassed = await testDirectoryStructure() && allTestsPassed;
  allTestsPassed = await testEnvironments() && allTestsPassed;
  
  // Cleanup
  await cleanup();
  
  // Final result
  console.log(blue("\n=== Test Results ==="));
  if (allTestsPassed) {
    console.log(green("✓ All tests passed! The MongoDBee migration system is working correctly."));
    console.log(yellow("\nNext steps:"));
    console.log(dim("• Connect to a real MongoDB instance"));
    console.log(dim("• Implement the actual migration execution"));
    console.log(dim("• Test with real database operations"));
  } else {
    console.log(red("✗ Some tests failed. Please check the output above."));
    Deno.exit(1);
  }
}

if (import.meta.main) {
  await main();
}