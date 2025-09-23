# MongoDBee Migration System - Complete Example

This playground demonstrates the complete MongoDBee migration system including:

- Configuration setup and management
- Migration file generation with templates
- CLI integration and commands
- Database connection handling
- Schema validation and migration execution

## Features Demonstrated

### 1. Configuration System
- Multi-environment configuration support
- Environment variable integration
- Path resolution and validation
- Connection settings management

### 2. Migration Generation
- Template-based migration creation
- Built-in templates (empty, create-collection, seed-data, transform-data, add-index)
- Custom variable injection
- Proper naming conventions and timestamping

### 3. CLI Integration
- JSR-compatible binary installation
- Cross-platform command execution
- Colored output and progress indicators
- Error handling and validation

### 4. Migration Types
- Collection creation and schema setup
- Data seeding and transformation
- Index management
- Schema validation integration

## Getting Started

### Prerequisites

Make sure you have:
- Deno installed (v1.40+)
- MongoDB running on localhost:27017 (or adjust configuration)
- Network access for JSR dependencies

### Quick Start

1. **Initialize the migration system:**
   ```bash
   deno task migrate:init
   ```

2. **Run the playground example:**
   ```bash
   deno task start
   ```

3. **Generate a new migration:**
   ```bash
   deno task migrate:generate --name create-users-collection --template create-collection
   ```

4. **Check migration status:**
   ```bash
   deno task migrate:status
   ```

5. **Apply migrations:**
   ```bash
   deno task migrate:apply
   ```

### Demo Mode

Run the complete demo:
```bash
deno task demo
```

This will:
1. Initialize the migration configuration
2. Run the playground example
3. Demonstrate all migration system features

## Configuration

The playground uses a sample configuration in `mongodbee.config.json`:

- **Database**: `mongodb://localhost:27017/playground_example`
- **Migrations**: `./migrations/` directory
- **Schemas**: `./schemas/` directory
- **Environment**: Development with verbose logging

## Migration Examples

The playground includes examples of:

1. **User Collection Setup** - Creates users collection with schema validation
2. **Initial Data Seeding** - Adds sample user data
3. **Index Creation** - Sets up performance indexes
4. **Data Transformation** - Updates existing data structures

## CLI Commands

All standard MongoDBee CLI commands are available:

- `deno task migrate:init` - Initialize configuration
- `deno task migrate:generate` - Generate new migration
- `deno task migrate:apply` - Apply pending migrations
- `deno task migrate:rollback` - Rollback last migration
- `deno task migrate:status` - Show migration status

## Architecture

This playground demonstrates:

- **Functional Design**: Pure functions throughout the migration system
- **Type Safety**: Full TypeScript integration with Valibot validation  
- **JSR Compatibility**: Following JSR best practices for Deno packages
- **Modular Structure**: Clean separation of concerns across modules
- **Configuration Flexibility**: Multi-environment support with overrides

## Next Steps

After exploring this playground:

1. Try creating custom migration templates
2. Experiment with different MongoDB configurations
3. Test multi-environment deployments
4. Integrate with your own project schemas

## Troubleshooting

Common issues:

- **MongoDB Connection**: Ensure MongoDB is running and accessible
- **Permissions**: CLI commands need `--allow-read --allow-write --allow-net --allow-env`
- **Dependencies**: Run `deno cache` if import issues occur
- **Configuration**: Check `mongodbee.config.json` for correct paths and settings