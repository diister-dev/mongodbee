/**
 * @fileoverview CLI Command System for MongoDBee Migrations
 * 
 * This module provides the command-line interface for the MongoDBee migration system,
 * including commands for generating, applying, and rolling back migrations.
 * 
 * @example
 * ```bash
 * # Generate a new migration
 * deno run -A cli.ts generate create-users --template create-collection
 * 
 * # Apply migrations  
 * deno run -A cli.ts apply --dry-run
 * 
 * # Rollback migrations
 * deno run -A cli.ts rollback --steps 2
 * 
 * # Show migration status
 * deno run -A cli.ts status
 * ```
 * 
 * @module
 */

import * as v from '../../schema.ts';

/**
 * Base CLI command interface
 */
export interface CLICommand {
  /** Command name */
  name: string;
  
  /** Command description */
  description: string;
  
  /** Command aliases */
  aliases?: string[];
  
  /** Available options for this command */
  options: CLIOption[];
  
  /** Execute the command */
  execute: (args: CLIArgs, context: CLIContext) => Promise<void>;
}

/**
 * CLI option definition
 */
export interface CLIOption {
  /** Option name (without dashes) */
  name: string;
  
  /** Option description */
  description: string;
  
  /** Short alias (single character) */
  alias?: string;
  
  /** Option type */
  type: 'string' | 'number' | 'boolean';
  
  /** Whether this option is required */
  required?: boolean;
  
  /** Default value */
  default?: unknown;
  
  /** Valid choices (for string options) */
  choices?: string[];
}

/**
 * Parsed CLI arguments
 */
export interface CLIArgs {
  /** The command to execute */
  command: string;
  
  /** Positional arguments */
  args: string[];
  
  /** Named options */
  options: Record<string, unknown>;
}

/**
 * CLI execution context
 */
export interface CLIContext {
  /** Current working directory */
  cwd: string;
  
  /** Environment variables */
  env: Record<string, string>;
  
  /** CLI configuration */
  config?: import('../config/types.ts').MigrationSystemConfig;
  
  /** Output functions */
  output: {
    log: (message: string, ...args: unknown[]) => void;
    error: (message: string, ...args: unknown[]) => void;
    warn: (message: string, ...args: unknown[]) => void;
    success: (message: string, ...args: unknown[]) => void;
    info: (message: string, ...args: unknown[]) => void;
  };
  
  /** Input functions */
  input: {
    prompt: (message: string, defaultValue?: string) => Promise<string>;
    confirm: (message: string, defaultValue?: boolean) => Promise<boolean>;
    select: (message: string, choices: { label: string; value: string }[]) => Promise<string>;
  };
}

/**
 * CLI execution result
 */
export interface CLIResult {
  /** Exit code */
  code: number;
  
  /** Output message */
  message?: string;
  
  /** Additional data */
  data?: Record<string, unknown>;
}

/**
 * Schema for CLI configuration
 */
export const CLIConfigSchema = v.object({
  /** Default configuration file path */
  configFile: v.optional(v.string()),
  
  /** Whether to use colors in output */
  colors: v.optional(v.boolean()),
  
  /** Output format */
  format: v.optional(v.picklist(['table', 'json', 'yaml'])),
  
  /** Verbosity level */
  verbose: v.optional(v.boolean()),
  
  /** Whether to show progress bars */
  progress: v.optional(v.boolean()),
});

/**
 * CLI configuration type
 */
export type CLIConfig = v.InferInput<typeof CLIConfigSchema>;

/**
 * Default CLI configuration
 */
export const DEFAULT_CLI_CONFIG: CLIConfig = {
  colors: true,
  format: 'table',
  verbose: false,
  progress: true,
};

/**
 * ANSI color codes for terminal output
 */
export const COLORS = {
  reset: '\x1b[0m',
  bright: '\x1b[1m',
  dim: '\x1b[2m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  magenta: '\x1b[35m',
  cyan: '\x1b[36m',
  white: '\x1b[37m',
  gray: '\x1b[90m',
} as const;

/**
 * Color helper functions
 */
export const colorize = {
  error: (text: string, useColors = true) => useColors ? `${COLORS.red}${text}${COLORS.reset}` : text,
  success: (text: string, useColors = true) => useColors ? `${COLORS.green}${text}${COLORS.reset}` : text,
  warn: (text: string, useColors = true) => useColors ? `${COLORS.yellow}${text}${COLORS.reset}` : text,
  info: (text: string, useColors = true) => useColors ? `${COLORS.blue}${text}${COLORS.reset}` : text,
  dim: (text: string, useColors = true) => useColors ? `${COLORS.dim}${text}${COLORS.reset}` : text,
  bright: (text: string, useColors = true) => useColors ? `${COLORS.bright}${text}${COLORS.reset}` : text,
  cyan: (text: string, useColors = true) => useColors ? `${COLORS.cyan}${text}${COLORS.reset}` : text,
};

/**
 * Parse command line arguments
 * 
 * @param args - Raw command line arguments (typically Deno.args)
 * @returns Parsed CLI arguments
 */
export function parseArgs(args: string[]): CLIArgs {
  const result: CLIArgs = {
    command: '',
    args: [],
    options: {},
  };
  
  let i = 0;
  
  // First argument is the command
  if (args.length > 0 && !args[0].startsWith('-')) {
    result.command = args[0];
    i = 1;
  }
  
  // Parse remaining arguments
  while (i < args.length) {
    const arg = args[i];
    
    if (arg.startsWith('--')) {
      // Long option: --option=value or --option value
      const [name, value] = arg.slice(2).split('=', 2);
      
      if (value !== undefined) {
        result.options[name] = value;
      } else if (i + 1 < args.length && !args[i + 1].startsWith('-')) {
        result.options[name] = args[i + 1];
        i++;
      } else {
        result.options[name] = true;
      }
    } else if (arg.startsWith('-') && arg.length > 1) {
      // Short option: -o value or -o
      const name = arg.slice(1);
      
      if (i + 1 < args.length && !args[i + 1].startsWith('-')) {
        result.options[name] = args[i + 1];
        i++;
      } else {
        result.options[name] = true;
      }
    } else {
      // Positional argument
      result.args.push(arg);
    }
    
    i++;
  }
  
  return result;
}

/**
 * Validate command arguments against command definition
 * 
 * @param args - Parsed CLI arguments
 * @param command - Command definition
 * @returns Validation errors (empty if valid)
 */
export function validateArgs(args: CLIArgs, command: CLICommand): string[] {
  const errors: string[] = [];
  
  // Check required options
  for (const option of command.options) {
    if (option.required) {
      const value = args.options[option.name] || args.options[option.alias || ''];
      
      if (value === undefined) {
        errors.push(`Required option --${option.name} is missing`);
      }
    }
  }
  
  // Validate option types and choices
  for (const [key, value] of Object.entries(args.options)) {
    const option = command.options.find(opt => opt.name === key || opt.alias === key);
    
    if (!option) {
      errors.push(`Unknown option --${key}`);
      continue;
    }
    
    // Type validation
    switch (option.type) {
      case 'number':
        if (isNaN(Number(value))) {
          errors.push(`Option --${option.name} must be a number`);
        }
        break;
      case 'boolean':
        if (typeof value !== 'boolean' && value !== 'true' && value !== 'false') {
          errors.push(`Option --${option.name} must be true or false`);
        }
        break;
      case 'string':
        if (option.choices && !option.choices.includes(String(value))) {
          errors.push(`Option --${option.name} must be one of: ${option.choices.join(', ')}`);
        }
        break;
    }
  }
  
  return errors;
}

/**
 * Create a CLI context for command execution
 * 
 * @param config - CLI configuration
 * @returns CLI execution context
 */
export function createCLIContext(config: CLIConfig = DEFAULT_CLI_CONFIG): CLIContext {
  const useColors = config.colors !== false;
  
  return {
    cwd: Deno.cwd(),
    env: Object.fromEntries(Object.entries(Deno.env.toObject())),
    
    output: {
      log: (message: string, ...args: unknown[]) => {
        console.log(message, ...args);
      },
      error: (message: string, ...args: unknown[]) => {
        console.error(colorize.error(`✗ ${message}`, useColors), ...args);
      },
      warn: (message: string, ...args: unknown[]) => {
        console.warn(colorize.warn(`⚠ ${message}`, useColors), ...args);
      },
      success: (message: string, ...args: unknown[]) => {
        console.log(colorize.success(`✓ ${message}`, useColors), ...args);
      },
      info: (message: string, ...args: unknown[]) => {
        console.info(colorize.info(`ℹ ${message}`, useColors), ...args);
      },
    },
    
    input: {
      async prompt(message: string, defaultValue?: string): Promise<string> {
        const prompt = defaultValue 
          ? `${message} (${colorize.dim(defaultValue, useColors)}): `
          : `${message}: `;
          
        const input = globalThis.prompt(prompt);
        return input || defaultValue || '';
      },
      
      async confirm(message: string, defaultValue = false): Promise<boolean> {
        const defaultText = defaultValue ? 'Y/n' : 'y/N';
        const prompt = `${message} (${colorize.dim(defaultText, useColors)}): `;
        
        const input = globalThis.prompt(prompt);
        
        if (!input) return defaultValue;
        
        return input.toLowerCase().startsWith('y');
      },
      
      async select(message: string, choices: { label: string; value: string }[]): Promise<string> {
        console.log(`\n${message}`);
        
        choices.forEach((choice, index) => {
          console.log(`  ${colorize.dim(`${index + 1}.`, useColors)} ${choice.label}`);
        });
        
        while (true) {
          const input = globalThis.prompt('\nSelect (number): ');
          
          if (!input) continue;
          
          const index = parseInt(input, 10) - 1;
          
          if (index >= 0 && index < choices.length) {
            return choices[index].value;
          }
          
          console.log(colorize.error('Invalid selection. Please try again.', useColors));
        }
      },
    },
  };
}

/**
 * Format table output for CLI
 * 
 * @param data - Array of objects to display as table
 * @param headers - Column headers (optional)
 * @returns Formatted table string
 */
export function formatTable(data: Record<string, unknown>[], headers?: string[]): string {
  if (data.length === 0) return '';
  
  const columns = headers || Object.keys(data[0]);
  const rows = data.map(row => columns.map(col => String(row[col] || '')));
  
  // Calculate column widths
  const widths = columns.map((col, i) => 
    Math.max(col.length, ...rows.map(row => row[i].length))
  );
  
  // Create separator line
  const separator = widths.map(width => '-'.repeat(width)).join(' | ');
  
  // Format header
  const header = columns.map((col, i) => col.padEnd(widths[i])).join(' | ');
  
  // Format rows
  const formattedRows = rows.map(row => 
    row.map((cell, i) => cell.padEnd(widths[i])).join(' | ')
  );
  
  return [header, separator, ...formattedRows].join('\n');
}

/**
 * Display help text for a command
 * 
 * @param command - Command to display help for
 * @param context - CLI context for output formatting
 */
export function displayHelp(command: CLICommand, context: CLIContext): void {
  const { output } = context;
  
  output.log(`\n${colorize.bright(command.name, true)} - ${command.description}\n`);
  
  if (command.aliases && command.aliases.length > 0) {
    output.log(`${colorize.dim('Aliases:', true)} ${command.aliases.join(', ')}\n`);
  }
  
  if (command.options.length > 0) {
    output.log(colorize.bright('Options:', true));
    
    for (const option of command.options) {
      const flags = [
        `--${option.name}`,
        option.alias ? `-${option.alias}` : null,
      ].filter(Boolean).join(', ');
      
      const required = option.required ? colorize.error(' (required)', true) : '';
      const defaultValue = option.default ? colorize.dim(` [default: ${option.default}]`, true) : '';
      const choices = option.choices ? colorize.dim(` [choices: ${option.choices.join(', ')}]`, true) : '';
      
      output.log(`  ${flags}  ${option.description}${required}${defaultValue}${choices}`);
    }
    
    output.log('');
  }
}

/**
 * Display general CLI help
 * 
 * @param commands - Available commands
 * @param context - CLI context for output formatting
 */
export function displayGeneralHelp(commands: CLICommand[], context: CLIContext): void {
  const { output } = context;
  
  output.log(`\n${colorize.bright('MongoDBee Migration CLI', true)}\n`);
  output.log('A type-safe MongoDB migration system with schema validation.\n');
  
  output.log(colorize.bright('Usage:', true));
  output.log('  mongodbee <command> [options]\n');
  
  output.log(colorize.bright('Available Commands:', true));
  
  const maxNameLength = Math.max(...commands.map(cmd => cmd.name.length));
  
  for (const command of commands) {
    const name = command.name.padEnd(maxNameLength);
    const aliases = command.aliases?.length ? colorize.dim(` (${command.aliases.join(', ')})`, true) : '';
    output.log(`  ${colorize.cyan(name, true)}${aliases}  ${command.description}`);
  }
  
  output.log('');
  output.log('Use "mongodbee <command> --help" for more information about a command.');
  output.log('');
}

/**
 * Execute a CLI command with error handling
 * 
 * @param command - Command to execute
 * @param args - Parsed CLI arguments  
 * @param context - CLI execution context
 * @returns CLI execution result
 */
export async function executeCommand(
  command: CLICommand,
  args: CLIArgs,
  context: CLIContext
): Promise<CLIResult> {
  try {
    // Check for help flag
    if (args.options.help || args.options.h) {
      displayHelp(command, context);
      return { code: 0 };
    }
    
    // Validate arguments
    const errors = validateArgs(args, command);
    if (errors.length > 0) {
      for (const error of errors) {
        context.output.error(error);
      }
      return { code: 1, message: 'Invalid arguments' };
    }
    
    // Execute command
    await command.execute(args, context);
    return { code: 0 };
    
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    context.output.error(`Command failed: ${message}`);
    
    if (args.options.verbose || args.options.v) {
      context.output.error(error instanceof Error ? error.stack || '' : '');
    }
    
    return { code: 1, message };
  }
}

/**
 * Main CLI runner function
 * 
 * @param args - Command line arguments
 * @param commands - Available CLI commands
 * @param config - CLI configuration
 * @returns Promise resolving to exit code
 */
export async function runCLI(
  args: string[],
  commands: CLICommand[],
  config: CLIConfig = DEFAULT_CLI_CONFIG
): Promise<number> {
  const parsedArgs = parseArgs(args);
  const context = createCLIContext(config);
  
  // Handle no command or help
  if (!parsedArgs.command || parsedArgs.command === 'help' || parsedArgs.options.help || parsedArgs.options.h) {
    if (parsedArgs.args.length > 0) {
      // Help for specific command
      const commandName = parsedArgs.args[0];
      const command = commands.find(cmd => 
        cmd.name === commandName || cmd.aliases?.includes(commandName)
      );
      
      if (command) {
        displayHelp(command, context);
      } else {
        context.output.error(`Unknown command: ${commandName}`);
        return 1;
      }
    } else {
      // General help
      displayGeneralHelp(commands, context);
    }
    return 0;
  }
  
  // Find and execute command
  const command = commands.find(cmd => 
    cmd.name === parsedArgs.command || cmd.aliases?.includes(parsedArgs.command)
  );
  
  if (!command) {
    context.output.error(`Unknown command: ${parsedArgs.command}`);
    context.output.info('Use "mongodbee --help" to see available commands.');
    return 1;
  }
  
  const result = await executeCommand(command, parsedArgs, context);
  return result.code;
}