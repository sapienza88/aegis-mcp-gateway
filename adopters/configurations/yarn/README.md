<!--
  Copyright 2026 Viquar Khan and the Aegis MCP Gateway contributors
  Licensed under the Apache License, Version 2.0
  https://www.apache.org/licenses/LICENSE-2.0
-->
# Apache Hadoop YARN profile

`yarn.yaml` governs Apache Hadoop YARN ResourceManager REST API reads (`/ws/v1/cluster/*`) plus
queue reassignment, priority updates, application kill, and scheduler queue refresh operations.

## Use it from Cursor or Kiro

Start your YARN ResourceManager (default port `8088`), then start Aegis and leave it listening on
`http://127.0.0.1:8080/mcp`. Configure your MCP client (`.cursor/mcp.json` or `.kiro/settings/mcp.json`)
to point at the running Aegis gateway URL with your bearer token—the agent calls Aegis, not the
ResourceManager REST endpoint directly.

## Why this source

The selected ResourceManager cluster, node, scheduler, and application surfaces are standard HTTP
JSON endpoints. Multi-step application submission (`POST /ws/v1/cluster/apps/new-application`
followed by `POST /ws/v1/cluster/apps` with full ApplicationMaster container launch context and
localized HDFS resources) and scheduler-specific node label mutations are omitted from this
declarative HTTP profile.

## Run

```powershell
java -jar aegis-gateway/target/aegis-gateway-2.0.0-SNAPSHOT-all.jar `
  --config adopters/configurations/yarn/yarn.yaml