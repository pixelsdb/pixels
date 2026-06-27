---
name: pixels-install
description: Guide, execute, and verify a basic Pixels deployment from docs/INSTALL.md using bundled helper scripts. Use when the user asks to install, configure, resume, validate, or troubleshoot a basic Pixels deployment or optional Trino setup.
---

# pixels-install

Help the user install a basic Pixels deployment according to `docs/INSTALL.md`.

Treat `docs/INSTALL.md` as the source of truth for the installation flow. Use the bundled scripts only as helpers for deterministic, repeatable, or easy-to-misconfigure steps. Do not treat this skill as a one-shot unattended installer.

## Resource Paths

- Skill directory: use the directory containing this `SKILL.md`.
- Helper scripts: `scripts/`.
- Shared helper scripts: `shared-scripts/`.
- Runtime state: `STATE_DIR` when set; otherwise the scripts resolve it to `.agents/state/pixels-install` for project use or `$HOME/.agents/state/pixels-install` for global installs.
- Pixels source tree: `REPO_ROOT` when set; otherwise scripts try to discover the current Git repository root and fail with a clear message if they cannot.

### Helper scripts

Use these helpers when they directly fit the current environment and the user-approved goal:

- `prepare_deployment.sh`: collect and confirm the Pixels deployment topology before any install writes to disk. With no node arguments it prompts for single-node vs. cluster, `PIXELS_HOME`, the coordinator node, whether the coordinator also runs a worker, and worker nodes; with arguments, use `--coordinator <ssh_target,host_ip,host_name,host_user,host_port>` plus repeated `--worker <...>` (legacy `--node` still works). Writes `STATE_DIR/deployment.env` and optionally delegates SSH setup to `shared-scripts/setup_cluster.sh`. In non-interactive mode, set `CONFIRM_PIXELS_DEPLOYMENT=true` only after the user has reviewed the chosen topology and paths.
- `check_prerequisites.sh`: validate OS, architecture, memory, disk, ports, host resolution, privilege, and optional SSH reachability. Runs every check and exits with a structured `=== check_prerequisites result ===` summary (one `ok|warn|fail|skip <check>: <detail>` line per check, plus a final `summary: ok=N warn=N fail=N skip=N status=pass|fail` line) instead of stopping at the first failure, so a single run shows every problem at once.
- `install_jdk.sh`: install a Zulu OpenJDK build (default JDK 23) matching the server's CPU architecture (x86_64 or aarch64) under `~/opt`, and persist `JAVA_HOME` only into the current user's shell profile. Looks for a JDK that already satisfies `JDK_VERSION` first — checking `JAVA_HOME` if set, otherwise resolving whatever `java` is on `PATH` (e.g. an `apt`-installed JDK that never exported `JAVA_HOME`) — and if one is found, skips the download entirely and only fixes up the environment variable to point at it. Only downloads/installs a fresh Zulu build when nothing on the system satisfies the version requirement. Falls back to pointing at the manual `.deb` method in `docs/INSTALL.md` if the Azul metadata API lookup fails.
- `install_maven.sh`: install or validate Maven 3.8+ (default pinned version `3.9.8`; override with `MAVEN_VERSION` only if the user explicitly asks for a different one) when the current Maven is missing or incompatible with the selected JDK. Same "skip if already satisfied" logic as `install_jdk.sh`: if an existing `mvn` (from `apt`, a prior manual install, etc.) already meets the minimum version, it reuses that installation's home directory instead of downloading anything. Fresh installs go into `~/opt/apache-maven-<version>` with a `~/opt/maven` symlink, never into `/opt`.
- `install_mysql.sh`: install MySQL, then interactively confirm the root password and the `pixels` metadata-DB-user password (default `password` for both, never applied silently), create the `pixels_metadata` database/user, and load `scripts/sql/metadata_schema.sql` from the Pixels source tree. It validates DB/user identifiers, quotes SQL values, avoids putting the root password in command-line arguments, and writes shell-quoted `STATE_DIR/deployment.secrets.env` (mode 600) so the confirmed credentials flow into `configure_pixels.sh` automatically.
- `install_etcd.sh`: install and optionally start the bundled etcd 3.3.4 package under `~/opt`. Always fixes the shipped `conf.yml`'s hardcoded `/home/ubuntu/...` `data-dir` to the real install path. By default (`ETCD_ALLOW_REMOTE=false`) it keeps etcd localhost-only. For a cluster, set `ETCD_ALLOW_REMOTE=true` only after confirming private networking/security-group rules; the script then requires `CONFIRM_ETCD_REMOTE_ACCESS=true`, `ASSUME_YES=true`, or an interactive confirmation before binding the client/peer listeners for remote access. Can also install and enable a `systemd` unit so etcd survives reboots and restarts on failure, but only after asking — it never installs this silently. Leave `INSTALL_ETCD_SYSTEMD_SERVICE` unset to be prompted interactively (`[y/N]`, defaults to no); set it to `true`/`false` to answer non-interactively once the user has actually agreed, or `ASSUME_YES=true` to answer yes to this and other yes/no prompts. Declining (or running non-interactively with no explicit answer) just skips the unit — etcd still starts, as a manually backgrounded process instead. Falls back the same way when `systemctl` isn't available at all. Restarts/re-enables the installed service automatically when the config actually changed.
- `build_install_pixels.sh`: build and install Pixels into the confirmed `PIXELS_HOME` from `deployment.env`, then add the MySQL JDBC connector. It prints the repo, deployment file, and install target before running; in non-interactive mode, set `CONFIRM_PIXELS_INSTALL=true` only after the user confirms the target. If you must bypass the prepared deployment file, pass `USE_DEPLOYMENT_FILE=false PIXELS_HOME=<path>` explicitly. `AUTO_CONFIRM_INSTALL` (default `true`) auto-answers `install.sh`'s prompts only when that is safe; if `PIXELS_HOME/etc/pixels.properties` or `pixels-cpp.properties` already exist, it forces the interactive path instead, because `install.sh` would otherwise prompt to add/remove config options or overwrite `pixels-cpp.properties` outright — exactly what `Claude agent`/`SKILL.md` say not to do without the user's say-so.
- `configure_pixels.sh`: update `PIXELS_HOME/etc/pixels.properties` after database, etcd, host, port, topology, and path values are known. Automatically sources `deployment.env` and `deployment.secrets.env` (if present), so `PIXELS_HOME`, coordinator service hosts, worker names, and MySQL credentials stay in sync with what the earlier scripts confirmed. It writes `$PIXELS_HOME/etc/workers` from `PIXELS_WORKERS` by default. It fails instead of silently writing the documented default DB password unless `METADATA_DB_PASSWORD` is explicit or `ALLOW_DEFAULT_METADATA_PASSWORD=true` is set.
- `install_shell_helpers.sh`: optional convenience step. Writes `start_pixels`/`stop_pixels`/`restart_pixels` shell functions to `~/.pixels-shell-helpers.sh` and sources that file from the current user's shell profile. These wrap `$PIXELS_HOME/sbin/{start,stop}-pixels.sh`; they only also drive Trino (`$TRINO_HOME/bin/launcher`) if the user's shell already has `TRINO_HOME` set, and skip that step (without failing) otherwise. Purely user-scoped (no system-wide change), so unlike the etcd systemd unit it does not need a confirmation prompt.
- `smoke_test.sh`: verify the installed layout, configuration, metadata access, etcd health, Pixels topology (`deployment.env` plus `$PIXELS_HOME/etc/workers`), and basic CLI behavior when applicable. It also checks Trino layout/catalog files when `CHECK_TRINO=true` and `trino-deployment.env` is present. Same structured-summary behavior as `check_prerequisites.sh` above.
- `progress.sh`: records which of `skill.yaml`'s `phases` have actually completed, in `STATE_DIR/progress.env`. `progress.sh mark <phase> [note]` after a phase succeeds, `progress.sh show` to see what's already done, `progress.sh is-done <phase>` to check one phase, `progress.sh reset` to start over. Exists so a session interrupted partway through a multi-phase install can resume from the actual state instead of guessing or re-running completed steps.
- `install_trino_cluster.sh`: run **on the coordinator** after `prepare_trino_cluster.sh`. It prints and confirms the Trino deployment file, version, install parent, home link, data dir, coordinator, coordinator scheduling mode, and workers before installing; in non-interactive mode, set `CONFIRM_TRINO_CLUSTER_INSTALL=true` only after the user confirms the fan-out. Installs Trino locally for the coordinator's own role, then dispatches `install_trino.sh` on every worker **in parallel** over the coordinator -> worker passwordless SSH that `prepare_trino_cluster.sh` already set up, instead of requiring the user or skill to copy the same command and run it by hand once per node. Copies the current `STATE_DIR/trino-deployment.env` to each worker first (workers don't need a shared filesystem), and assumes the repository already exists at the same path on every worker (override `REMOTE_REPO_ROOT` otherwise). Emits the same structured per-node summary as `smoke_test.sh`/`check_prerequisites.sh`, with a log file per node under `STATE_DIR/logs/` for diagnosing any failure.
- `prepare_trino_cluster.sh`: collects and confirms the Trino topology and install locations: one coordinator, zero or more workers, whether the coordinator also accepts scheduled work, Trino version, HTTP port, install parent, home symlink/current path, and data directory. With no node arguments it prompts interactively; with arguments, use `--coordinator <ssh_target,host_ip,host_name,host_user,host_port>` plus repeated `--worker <...>`. Writes `STATE_DIR/trino-deployment.env`. Optionally delegates to `shared-scripts/setup_cluster.sh` (`--setup-ssh true`) so the coordinator and every worker can passwordlessly SSH into each other, which the cluster shell helpers below depend on. In non-interactive mode, set `CONFIRM_TRINO_DEPLOYMENT=true` only after the user has reviewed the topology and paths.
- `install_trino.sh`: installs Trino (per the [466 deployment docs](https://trino.io/docs/466/installation/deployment.html)) on the **current node only** — run it once per node in the cluster, it does not SSH anywhere itself (use `install_trino_cluster.sh` to fan it out across the cluster instead of running it by hand on each node). It reads `trino-deployment.env`, validates this node's role (`coordinator` or `worker`) and the coordinator host, and confirms the role plus install/data paths before writing files; in non-interactive mode, set `CONFIRM_TRINO_INSTALL=true` only after the user confirms the target. Downloads `trino-server-<version>.tar.gz` (default 466) from Maven Central into the confirmed install parent with the confirmed home symlink (`change_trino_version`-friendly). Writes `etc/node.properties` (`node.data-dir` under the confirmed data dir, outside the versioned install dir so it survives version switches; `node.id` generated once and preserved across re-runs), `etc/jvm.config` (official defaults plus the two `--add-opens` flags `pixels-trino` requires), `etc/log.properties`, and `etc/config.properties` — `discovery.uri` always points at the coordinator's real IP from `trino-deployment.env`, never `localhost`; this node's coordinator/worker role is read from `trino-deployment.env` or inferred from this host's own IPs, overridable with `TRINO_ROLE`. Also clones/builds `pixelsdb/pixels-trino` (requires Pixels already `mvn install`ed locally and JDK 23) and installs the connector into `plugin/`, writing `etc/catalog/pixels.properties` (`cloud.function.switch=off` by default — Pixels Turbo stays out of scope unless asked). The event listener plugin is optional and off by default (`INSTALL_PIXELS_TRINO_LISTENER=true` to enable). Best-effort installs `trino-cli` into `bin/trino`.
- `install_trino_shell_helpers.sh`: optional, and — unlike `install_shell_helpers.sh` — **always asks first**, because it bakes a specific list of remote hostnames (from `trino-deployment.env`) into the user's shell profile and assumes passwordless SSH between them is already set up. Writes `start_trino_cluster`/`stop_trino_cluster`/`restart_trino_cluster`/`change_trino_version`/`trino_cli` to `~/.trino-shell-helpers.sh`: the coordinator is always operated locally, every worker over SSH. Leave `INSTALL_TRINO_SHELL_HELPERS` unset to be prompted `[y/N]`; set `true`/`false` to answer non-interactively once the user has actually agreed, or `ASSUME_YES=true`.

All of these scripts anchor paths on `$HOME` (e.g. `~/opt/...`) and detect the
current login shell (`$SHELL`) to pick `~/.bashrc` or `~/.zshrc` before
persisting any `export`. Never assume the deployment user is named `pixels`
or `ubuntu`, and never write environment variables into a global file such
as `/etc/environment` — `scripts/lib/shell_env.sh`
centralizes this logic and is shared by `install_jdk.sh`, `install_maven.sh`,
`install_etcd.sh`, `build_install_pixels.sh`, `configure_pixels.sh`,
`check_prerequisites.sh`, `smoke_test.sh`, `install_trino.sh`,
`install_trino_cluster.sh`, `prepare_trino_cluster.sh`,
`install_trino_shell_helpers.sh`, and `progress.sh`. It also defines the
structured-result (`result_record`/`result_emit_summary`) and phase-checkpoint
(`mark_phase_done`/`phase_is_done`/`print_progress`) helpers described above.

Each `install_*.sh` script runs as its own process, so exporting a variable
in one script does not make it visible to the next one. `docs/INSTALL.md`'s
own walkthrough handles this by running `source ~/.bashrc` right after
appending to it, but sourcing the user's real rc file from a *non*-interactive
script is unreliable (Ubuntu's default `~/.bashrc` returns immediately for
non-interactive shells, before reaching anything appended at the bottom). So
in addition to persisting into the user's real shell profile,
`shell_env.sh` also mirrors `JAVA_HOME`/`MAVEN_HOME`/`ETCD`/`PIXELS_HOME`
into a small `STATE_DIR/toolchain.env` file with no such guard,
and every script sources it (`load_toolchain_env`) at startup. This means
running `install_jdk.sh` then `install_maven.sh` as two separate tool calls
still works without the skill needing to manually re-source anything in
between — but if you set any of these variables yourself before running a
step, your explicit environment variable still wins.

The skill may run simple commands from `docs/INSTALL.md` directly when that is clearer than adding another wrapper script.

### Workflow

The phase sequence is fixed in `skill.yaml`'s `phases` list — this section is decision guidance for each phase, not a second copy of that ordering. Before doing anything else, run `scripts/progress.sh show` to see which phases (if any) already completed in a prior session, and skip straight to the first phase that is not yet marked done instead of re-deriving state from scratch or blindly redoing finished work. After each phase actually succeeds, run `scripts/progress.sh mark <phase>` (phase names match `skill.yaml`) so a later session — or this one, after a long gap — can pick up correctly.

- **discover_install_target**: clarify single node vs. cluster, `PIXELS_HOME`, coordinator vs. worker roles, storage/query engine needs, whether to start services now, and whether optional components are in scope. Determine the actual login user (`whoami`) and home directory (`$HOME`) instead of assuming `pixels` or `ubuntu`; every path this skill writes is anchored on that `$HOME`. Do not proceed with defaults until the user has confirmed the topology and install locations.
- **check_prerequisites**: run `check_prerequisites.sh` and read its full structured summary before deciding what to install — see "Failure Handling" below for what to do with anything it reports as `fail`.
- **prepare_deployment_config**: run for both single-node and cluster deployments so later scripts read the same confirmed `PIXELS_HOME`, coordinator service hosts, and worker list from `deployment.env`. Prefer `prepare_deployment.sh` with no node arguments for an interactive prompt, or explicit `--coordinator ... --worker ...` arguments when the user has already provided the topology.
- **install_or_verify_jdk**: `install_jdk.sh` already skips the download when anything on the system (apt-installed JDK, a previous run, etc.) satisfies `JDK_VERSION`; it only selects and installs a Zulu build under `~/opt` when nothing qualifies. JDK 23+ is required for the documented Pixels + Trino 466 path. Ask before installing a downloaded JDK package.
- **install_or_verify_maven**: `install_maven.sh` has the same "skip if already satisfied" behavior for Maven 3.8+ (default pinned `3.9.8`, only overridden if the user asks). Ensure `mvn -v` ends up using the selected JDK.
- **install_or_verify_mysql** / **install_or_verify_etcd**: only after the user confirms credentials/ports (see Guardrails — never apply the default MySQL password silently); `install_etcd.sh` keeps localhost-only by default, asks before remote etcd access, and asks before installing its `systemd` auto-start unit.
- **build_install_pixels**: install Pixels from the repository into the confirmed `PIXELS_HOME` from `deployment.env` and place the MySQL JDBC connector under `PIXELS_HOME/lib`.
- **configure_pixels**: update `pixels.properties` only once paths, hosts, ports, database, etcd, topology, and cache settings are confirmed; it derives `pixels.var.dir` from the resolved `PIXELS_HOME`, writes `$PIXELS_HOME/etc/workers` from `PIXELS_WORKERS`, and reuses the MySQL credentials `install_mysql.sh` already confirmed. If MySQL was configured elsewhere, pass `METADATA_DB_PASSWORD` explicitly.
- **start_pixels_optional**: only when the user asks to start services or startup is part of the requested task.
- **install_shell_helpers_optional**: only if the user wants `start_pixels`/`stop_pixels`/`restart_pixels` convenience functions.
- **smoke_test**: run after configuration or startup changes; read the full structured summary (see "Failure Handling" below), don't just check the exit code. Use `CHECK_TRINO=true` only when Trino was installed or explicitly requested.
- **optional_trino_install**: only when the user explicitly asks. Clarify the topology first: coordinator vs. worker(s), how many workers, whether the coordinator also accepts scheduled work (`node-scheduler.include-coordinator`; official docs recommend `false` for a dedicated coordinator, `true` for a single-node test setup), `TRINO_INSTALL_PARENT`, `TRINO_HOME_LINK`, and `TRINO_DATA_DIR`. Run `prepare_trino_cluster.sh --setup-ssh true`, then run `install_trino_cluster.sh` **on the coordinator** to install Trino on the coordinator and fan out to every worker in parallel over SSH, instead of repeating `install_trino.sh` by hand once per node. Configure the catalog and `presto.pixels.jdbc.url` only once the coordinator's endpoint and catalog/schema are known. Offer `install_trino_shell_helpers.sh` only after the user agrees (it asks by default anyway).
- **optional_hadoop_or_monitoring**: out of scope unless the user asks — see Optional Components below.

### Failure handling

`check_prerequisites.sh` and `smoke_test.sh` never stop at the first failed check — they run every check and print a structured summary block (`=== <script> result ===` … `summary: ok=N warn=N fail=N skip=N status=pass|fail` … `=== end <script> result ===`). When `status=fail`:

1. Read every `fail` line in the block, not just the first — they are independent and often have independent causes (e.g. a port conflict and a missing config property are unrelated problems, both worth knowing about before acting).
2. For each `fail`, decide whether it is something this skill should fix directly (e.g. an expected service genuinely isn't running yet — wait and re-check), something that needs a user decision (e.g. a port is held by an unrelated process — ask whether to stop it or change Pixels' port), or something informational that the user already knows about (e.g. a deliberately non-Ubuntu host).
3. Don't re-run the same script in a loop hoping it passes — fix (or get a decision on) the specific `fail` line first, then re-run once to confirm.
4. `warn` and `skip` lines never fail the run by themselves, but still read them — a `warn` is frequently the first sign of an edge case worth flagging to the user even though it didn't block anything.

The same applies to `install_trino_cluster.sh`'s per-node summary: a `fail` for one worker doesn't mean the whole cluster install failed — check which specific node(s) failed, read that node's log under `STATE_DIR/logs/`, and retry only that node rather than re-running the whole fan-out.

### Optional components

Do not install these unless the user explicitly asks for the corresponding later stage:

- Trino or another query engine — see the dedicated Trino steps above and the scripts list; this skill can now install Trino itself when asked, instead of only pointing at the external `pixels-trino` documentation.
- Hadoop or HDFS configuration.
- AWS credentials.
- Prometheus, Grafana, node exporter, or JMX exporter.
- Pixels Cache, Turbo, Retina, or Amphi.

The basic Pixels install flow may configure `presto.pixels.jdbc.url` only after the Trino endpoint and catalog/schema values are known.

### Examples

Prepare a three-node Pixels cluster config and SSH (run from this skill directory, or invoke the script by absolute path):

```bash
./scripts/prepare_deployment.sh \
  --ssh-user root \
  --setup-ssh true \
  --coordinator-is-worker false \
  --pixels-home /home/pixels/opt/pixels \
  --coordinator 10.0.0.11,10.0.0.11,pixels-coordinator,root,22 \
  --worker 10.0.0.12,10.0.0.12,pixels-worker-1,root,22 \
  --worker 10.0.0.13,10.0.0.13,pixels-worker-2,root,22
```

```bash
./scripts/check_prerequisites.sh
```

Prepare a 1-coordinator/2-worker Trino cluster, SSH, then install Trino on every node in parallel (run on the coordinator):

```bash
./scripts/prepare_trino_cluster.sh \
  --ssh-user root \
  --setup-ssh true \
  --coordinator 10.0.0.11,10.0.0.11,trino-coordinator,root,22 \
  --worker 10.0.0.12,10.0.0.12,trino-worker-1,root,22 \
  --worker 10.0.0.13,10.0.0.13,trino-worker-2,root,22

./scripts/install_trino_cluster.sh
```

Check (and resume from) the recorded install progress:

```bash
./scripts/progress.sh show
./scripts/progress.sh mark check_prerequisites
```

### Guardrails

- Prefer `docs/INSTALL.md` plus targeted helper scripts over a fixed all-in-one install command.
- Do not run scripts that install packages, change services, update credentials, or start daemons unless the user is asking to perform that installation stage.
- Do not modify Git state.
- When modifying this skill's source, keep changes limited to `skills/pixels-install/` unless the user explicitly requests more.
- Ask before handling secrets, installing downloaded JDK packages, changing MySQL root authentication, enabling remote database access, installing the etcd `systemd` auto-start unit, or installing the Trino cluster shell helpers.
- Ask or use the interactive preparation scripts before choosing `PIXELS_HOME`, Trino install/data paths, single-node vs. cluster mode, coordinator role, worker list, or whether a coordinator also runs worker tasks. Never let the skill silently accept topology/path defaults on the user's behalf.
- In non-interactive runs, only set `CONFIRM_PIXELS_DEPLOYMENT=true`, `CONFIRM_PIXELS_INSTALL=true`, `CONFIRM_TRINO_DEPLOYMENT=true`, `CONFIRM_TRINO_INSTALL=true`, or `CONFIRM_TRINO_CLUSTER_INSTALL=true` after showing the user the exact topology and paths that will be written or installed.
- Never hardcode `/home/pixels`, `/home/ubuntu`, or any other specific home directory in a command; always resolve paths from `$HOME` for whichever user is actually running the install.
- Never persist `JAVA_HOME`, `MAVEN_HOME`, `ETCD`, or `PIXELS_HOME` into a global file (`/etc/environment`, `/etc/profile.d/*`); only append to the current user's shell profile, matching their actual login shell.
- Never accept the documented default MySQL passwords (`password`) without an explicit interactive confirmation from the user; `install_mysql.sh` enforces this by prompting even when a default is acceptable.
- Never run `install_trino.sh` (directly or via `install_trino_cluster.sh`) against a node without first knowing its role (coordinator vs. worker) from `trino-deployment.env` or an explicit `TRINO_ROLE`; `discovery.uri` must point at the coordinator's real IP, never `localhost`.
- When a structured result summary reports `status=fail`, address (or get a user decision on) every `fail` line before re-running — see "Failure handling" above. Don't paper over a `fail` by re-running the script in a loop.
- `STATE_DIR/deployment.env`, `STATE_DIR/deployment.secrets.env`, `STATE_DIR/toolchain.env`, `STATE_DIR/trino-deployment.env`, `STATE_DIR/progress.env`, and `STATE_DIR/logs/` are generated, user-specific, and must never be committed.
