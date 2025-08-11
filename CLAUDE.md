# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Snowplow Snowflake Loader, a Scala-based streaming application that loads Snowplow enriched events into Snowflake with low latency. The codebase follows functional programming paradigm using Cats Effect for effect management and FS2 for streaming. The project supports three cloud platforms:
- **Azure**: Reads from Event Hubs (Kafka) 
- **AWS**: Reads from Kinesis streams
- **GCP**: Reads from Pub/Sub

## Development Commands

### Building
```bash
# Compile all modules
sbt compile

# Compile specific module
sbt core/compile
sbt kafka/compile
sbt kinesis/compile  
sbt pubsub/compile
```

### Testing
```bash
# Run all tests
sbt test

# Run tests for specific module
sbt core/test
sbt kafka/test
sbt kinesis/test
sbt pubsub/test

# Run specific test class
sbt "core/testOnly *ProcessingSpec"
```

### Formatting and Linting
```bash
# Check code formatting
sbt scalafmtCheck

# Format code
sbt scalafmt

# Check headers
sbt headerCheck

# Add headers
sbt headerCreate
```

### Docker Images
```bash
# Build Docker images for Ubuntu base
sbt kafka/docker:publishLocal
sbt kinesis/docker:publishLocal
sbt pubsub/docker:publishLocal

# Build distroless Docker images
sbt kafkaDistroless/docker:publishLocal
sbt kinesisDistroless/docker:publishLocal
sbt pubsubDistroless/docker:publishLocal
```

### Running Applications
```bash
# Run with config file
docker run \
  -v /path/to/config.hocon:/var/config.hocon \
  snowplow/snowflake-loader-{kafka|kinesis|pubsub}:VERSION \
  --config /var/config.hocon
```

## Architecture

### Multi-Module Structure
- **core/**: Shared business logic for Snowflake loading, configuration, processing pipeline
- **kafka/**: Azure Event Hubs integration with separate auth handlers for source/sink
- **kinesis/**: AWS Kinesis integration 
- **pubsub/**: GCP Pub/Sub integration
- **distroless/**: Distroless Docker variants for each platform

### Key Components

#### Core Processing (`modules/core/`)
- `LoaderApp.scala`: Abstract base for platform-specific applications
- `Config.scala`: Configuration data structures with Circe decoders
- `processing/Processing.scala`: Main event processing pipeline using FS2 streams
- `processing/TableManager.scala`: Snowflake table schema management
- `processing/Channel.scala`: Snowflake channel management for parallel uploads

#### Platform Applications
- `kafka/AzureApp.scala`: Azure-specific entry point with Event Hubs auth handlers
- `kinesis/AwsApp.scala`: AWS-specific entry point
- `pubsub/GcpApp.scala`: GCP-specific entry point

### Configuration
- Base config: `modules/core/src/main/resources/reference.conf`
- Example configs: `config/` directory with minimal and reference configs per platform
- Uses HOCON format with environment variable substitution
- License acceptance required via `ACCEPT_LIMITED_USE_LICENSE` env var

### Build Configuration
- Scala 2.13.16 with Cats Effect ecosystem (FS2, Http4s, Doobie)
- SBT multi-project build with shared settings in `project/BuildSettings.scala`
- Dependencies managed in `project/Dependencies.scala` with version overrides for security
- Plugins: scalafmt, sbt-header for license headers, BuildInfo, Docker packaging

### Authentication
- **Azure**: Uses separate `AzureAuthenticationCallbackHandler` instances for source and sink Kafka connections
- **AWS**: Uses STS SDK for credential management  
- **Snowflake**: Private key authentication with optional passphrase

### Processing Pipeline
- Reads enriched events from cloud message queues
- Transforms events to Snowflake-compatible format
- Batches events for efficient loading
- Handles schema evolution by detecting new columns and altering tables
- Uses Snowflake channels for parallel, transactional uploads
- Implements comprehensive retry logic for transient failures

### Testing
- Specs2 test framework with Cats Effect integration
- Mock environments for testing (`MockEnvironment.scala`)
- Config parsing tests for each platform
- Processing pipeline unit tests