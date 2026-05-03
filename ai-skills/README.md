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

### As a plugin (most agent runtimes)

Most agent harnesses (Claude Code, Cursor, etc.) accept a folder of
`SKILL.md` files via a plugin manifest. Point your tool at this
folder; the skills in `skills/` will be picked up automatically.

```text
# Claude Code (example)
/plugin install /path/to/rakka/ai-skills
```

### By copying

If your tooling expects skills under a project-local directory, copy
them in:

```bash
cp -r /path/to/rakka/ai-skills/skills/* .claude/skills/
# or wherever your assistant looks for SKILL.md files
```

### By symlink (track upstream)

```bash
ln -s /path/to/rakka/ai-skills/skills/rakka-actor-design \
      .claude/skills/rakka-actor-design
```

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
