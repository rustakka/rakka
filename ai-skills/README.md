# ai-skills/

Skills for AI coding assistants working on **projects that depend on
rakka** — not for editing rakka itself. They follow the standard
`SKILL.md` + frontmatter convention used by Claude Code, Claude Agent
SDK, and other agentic tools.

These skills are deliberately separate from the repo's own dev tooling
(`.claude/`, `xtask/`, etc.) so that distributing them to consumers
does not entangle rakka's internal development workflow.

## What's here

| Skill | Use when… |
|---|---|
| `rakka-actor-design` | Authoring or modifying an `impl Actor` — Msg types, supervision, lifecycle, FSM patterns |
| `rakka-testing` | Writing tests against rakka actors using `rakka-testkit` |
| `rakka-troubleshooting` | Debugging rakka-flavored errors — mailbox backpressure, missing features, restart loops, split-brain |
| `rakka-cluster` | Bringing up clustering, sharding, singleton, pub/sub, distributed data |
| `rakka-persistence` | Event sourcing — journals, snapshots, recovery, picking a storage adapter |
| `rakka-python` | Using the Python bindings — GIL strategy, async ask/tell, mixing with Rust actors |

Each `SKILL.md` is a thin router: it points at canonical docs in this
repo (`docs/*.md`, `examples/*`) and at the relevant crate's API. It
deliberately does **not** restate API surfaces that belong in rustdoc,
because those drift faster than docs.

## Installing

Pick the path that matches your assistant. The skills themselves are
vendor-neutral `SKILL.md` files — only the install mechanism differs.

### Claude Code (recommended: marketplace)

If you use Claude Code, install via the plugin marketplace — this
keeps the skills updated as rakka releases, with no manual copy step:

```text
/plugin marketplace add rustakka/rakka
/plugin install rakka-ai-skills@rakka
```

You can also install from a local checkout (useful when developing
against a rakka fork):

```text
/plugin marketplace add /path/to/rakka
/plugin install rakka-ai-skills@rakka
```

Skills auto-activate based on the `description` frontmatter — no need
to invoke them explicitly.

### Claude Agent SDK / project-local `.claude/skills/`

For SDK-based agents or project-local Claude Code setups that read
from `.claude/skills/`, copy or symlink the skills in:

```bash
# copy (snapshot)
cp -r /path/to/rakka/ai-skills/skills/* .claude/skills/

# symlink (track upstream)
ln -s /path/to/rakka/ai-skills/skills/rakka-actor-design \
      .claude/skills/rakka-actor-design
```

### Cursor

Cursor reads project rules from `.cursor/rules/`. Copy the skills in
as `.mdc` rules; Cursor will treat the frontmatter `description` as
the activation hint:

```bash
mkdir -p .cursor/rules
for s in /path/to/rakka/ai-skills/skills/*/SKILL.md; do
  name=$(basename "$(dirname "$s")")
  cp "$s" ".cursor/rules/${name}.mdc"
done
```

### OpenAI Codex CLI

Codex CLI reads `AGENTS.md` (project-level) and `~/.codex/AGENTS.md`
(user-level). It does not have a SKILL.md loader, so reference the
skills from `AGENTS.md` and let the model pull them in on demand:

```markdown
<!-- in AGENTS.md -->
## rakka skills
When working on rakka actors, consult the matching skill in
`ai-skills/skills/<name>/SKILL.md`:
- actor design / supervision → rakka-actor-design
- tests with rakka-testkit   → rakka-testing
- cluster / sharding / pubsub → rakka-cluster
- event sourcing / journals  → rakka-persistence
- Python bindings            → rakka-python
- mailbox / restart / errors → rakka-troubleshooting
```

### Gemini CLI

Gemini CLI reads `GEMINI.md` and supports custom commands under
`.gemini/commands/`. Point Gemini at the skills the same way:

```markdown
<!-- in GEMINI.md -->
For rakka work, load the relevant skill from
`ai-skills/skills/<name>/SKILL.md` before editing.
```

Optionally wrap each skill as a slash command in
`.gemini/commands/rakka-<name>.toml` whose `prompt` includes
`@ai-skills/skills/<name>/SKILL.md`.

### Other harnesses (Aider, Continue, Zed, etc.)

Any tool that accepts a system prompt or rules file can load these
skills — `SKILL.md` is plain Markdown with YAML frontmatter. Either
include the file directly in the system prompt, or reference its path
from your tool's rules file (`.aider.conf.yml`, `.continue/`, etc.).

## Authoring conventions

- **One job per skill.** A skill is a router into the right docs +
  examples for one task. If a skill is trying to teach two things, it
  should be two skills (or it should defer to docs).
- **Defer to source-of-truth docs.** Link to `docs/*.md` and
  `examples/*` rather than restating them. Skills go stale; docs
  travel with the code.
- **Vendor-neutral.** No references to a specific assistant, harness,
  or tool. Describe rakka, not the runtime loading the skill.
- **Frontmatter.** Each skill begins with `---` frontmatter containing
  `name` and `description`. The description is a one-line activation
  hint — what the user is doing when this skill should kick in.

## Versioning

These skills version with the repo. If a release changes a public API
covered by a skill, update the skill in the same PR. The skills are
not separately published.
