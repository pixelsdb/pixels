## Skills Development Directory

`skills/` is the source directory for optional AI-assisted workflows in this
project. Nothing in this directory is enabled automatically for users who clone
Pixels; users explicitly install a skill with `skills/install.sh`.

### Layout

- `skills/`: Source directory for installable skills.
- `skills/<skill-name>/`: One independent skill development directory.
- `skills/<skill-name>/skill.yaml`: Skill metadata, script map, and phase list.
- `skills/<skill-name>/codex/SKILL.md`: Codex skill entrypoint.
- `skills/<skill-name>/claude/agent.md`: Claude Code agent entrypoint, when supported.
- `skills/<skill-name>/scripts/`: Runtime scripts bundled with the installed skill.
- `skills/<skill-name>/scripts/lib/`: Shell helpers sourced by that skill's own scripts.
- `skills/<skill-name>/shared-scripts/`: Shared helper scripts bundled into that skill.
- `skills/<skill-name>/references/`: Optional reference files loaded only when needed.

### Commands

- `./skills/list.sh`: List skill directories that contain `skill.yaml`.
- `./skills/install.sh --skill <name> --tool <codex|claude> --scope <project|global>`: Install a skill for the selected tool and scope.
- `./skills/uninstall.sh --skill <name> --tool <codex|claude> --scope <project|global>`: Remove an installed skill for the selected tool and scope.

`--agent <name>` is accepted by install/uninstall as a deprecated alias for
`--skill <name>` during the migration from the old `agents/` layout.

### Install Behavior

Codex project-scope installs copy the selected skill into Codex's repository
skill discovery path:

```text
<repo>/.agents/skills/<skill-name>/
```

Codex global-scope installs copy the selected skill into Codex's user skill
discovery path:

```text
$HOME/.agents/skills/<skill-name>/
```

Claude project/global installs copy the Claude markdown agent to the matching
`.claude/agents/` directory and copy scripts/references to a companion assets
directory:

```text
<repo>/.claude/agent-assets/<skill-name>/
$HOME/.claude/agent-assets/<skill-name>/
```

Runtime state is not written into this development directory or into the
installed skill bundle. The pixels-install scripts default to:

```text
<repo>/.agents/state/pixels-install/
$HOME/.agents/state/pixels-install/
```

Override with `STATE_DIR=<path>` when a different state location is required.
