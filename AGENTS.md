# AGENTS.md

## Purpose and scope
- This repository is a multi-module Java + C++ codebase for the Pixels columnar engine, deployment daemons, and serverless query acceleration.
- `PIXELS_HOME` is a hard requirement for most workflows (`install.sh`, IntelliJ run configs, runtime scripts).
- Existing AI guidance files were not found; conventions here are inferred from project READMEs and build scripts.

## Big-picture architecture (read these first)
- Core format/runtime: `pixels-core`, shared APIs/types: `pixels-common`, cache service: `pixels-cache`.
- Control plane services run as stateless daemons (`pixels-daemon`): metadata, transaction, cache coordination, index endpoints.
- External API gateway: `pixels-server` (REST on `18891`, RPC on `18892`).
- Query pipeline: SQL parse (`pixels-parser`) -> physical planning (`pixels-planner`) -> operator execution (`pixels-executor`).
- Turbo/serverless path: `pixels-turbo/*` modules (`pixels-invoker-*`, `pixels-worker-*`, `pixels-scaling-*`) integrate Trino with Lambda/vHive/EC2 autoscaling.
- Retina CDC path: `pixels-retina` + `cpp/pixels-retina` replay log-based changes with MVCC; index backends are pluggable under `pixels-index/*`.

## Cross-component contracts and data flow
- RPC/data contracts are centralized in `proto/*.proto`; row batch schema is in `flatbuffers/rowBatch.fbs`.
- Storage backends are split by module (`pixels-storage/pixels-storage-{s3,hdfs,gcs,redis,http,localfs,...}`) and selected by table/storage settings.
- Typical operational flow: Trino connector reads Pixels metadata/files -> optional Turbo pushdown builds sub-plans -> workers execute and write intermediate/output storage.
- Example runtime switch for Turbo lives in Trino catalog config: `cloud.function.switch=off|on|auto` (`pixels-turbo/README.md`).

## Developer workflows (project-specific)
- Full build from repo root (default used by project):
  - `mvn -T 3 clean install`
- Install local runnable layout (`bin/`, `sbin/`, `etc/`) into `PIXELS_HOME`:
  - `./install.sh`
- Start core services after install:
  - `./sbin/start-pixels.sh` (from `PIXELS_HOME`)
- Run CLI tooling (load/compact/stat/eval):
  - `java -jar ./sbin/pixels-cli-*-full.jar`
- C++/DuckDB path is separate (`cpp/README.md`):
  - `cd cpp && make pull && make -j`

## Test/build caveats to avoid wasted cycles
- Root `pom.xml` sets `maven-surefire-plugin` with `<skipTests>true</skipTests>`; tests do not run unless explicitly enabled.
- Some JUnit tests need lower JDK internals access (README notes JDK 8 for those tests), while integrations like Trino may require newer JDKs.
- Prefer module-scoped iterations when changing one area (for example `mvn -pl pixels-core -am ...`) to avoid rebuilding all modules.

## Conventions agents should follow in this repo
- Keep runtime/config edits aligned with `PIXELS_HOME/etc/pixels.properties` and scripts under `scripts/{bin,sbin,etc}`.
- When documenting/evaluating performance flows, mirror project examples in `docs/TPC-H.md` and `docs/CLICKBENCH.md` (for example `LOAD`, `COMPACT`, `STAT`).
- For storage- or index-related changes, update the matching pluggable module instead of hard-coding backend-specific logic in shared modules.
- For serverless changes, ensure planner/invoker/worker settings remain consistent (input/intermediate/output storage schemes in `pixels-turbo/README.md`).

