## Agents Source Directory

`agents/` is the source of truth for maintaining, installing, and distributing AI agents in this project.

### Layout

- `agents/`: Source directory for all agents.
- `agents/<agent-name>/`: One independent directory per agent.
- `agents/<agent-name>/agent.yaml`: Agent metadata, target configuration, and dependencies.
- `agents/<agent-name>/claude.md`: Source file used when installing the agent to Claude Code.
- `agents/<agent-name>/codex.md`: Source file used when installing the agent to Codex.
- `agents/skills/`: Shared skills that can be reused by multiple agents.
- `agents/scripts/`: Shared runtime scripts that can be reused by multiple agents.
- `agents/<agent-name>/skills/`: Private skills used only by that agent.
- `agents/<agent-name>/scripts/`: Private runtime scripts used only by that agent.
- `agents/scripts/`: Helper scripts used internally by the installer.

### Commands

- `./agents/list.sh`: List agent directories that contain `agent.yaml`.
- `./agents/install.sh --agent <name> --tool <claude|codex> --scope <project|global>`: Install an agent for the selected tool and scope.
- `./agents/uninstall.sh --agent <name> --tool <claude|codex> --scope <project|global>`: Remove an installed agent for the selected tool and scope.
