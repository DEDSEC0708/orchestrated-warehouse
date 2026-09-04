# ADR-016: Secret scanning is a test in the suite, not a review step

- **Status:** Accepted
- **Date:** 2026-09-04 (Phase 12)
- **Related:** ADR-007 (least-privilege roles and environment configuration)

## Context

The project's configuration rule is that credentials come from the
environment: `.env` is gitignored, `Settings` requires the passwords with no
defaults, `.env.example` holds only placeholder shapes, and the PostgreSQL
init scripts pass secrets through `psql --set` rather than writing them into a
committed `.sql` file.

That rule is enforced by nobody. It was verified by grepping the tree once
during review, which proves the repository was clean **once**. The failure
mode is well known: someone pastes a real DSN into a debugging line, the pull
request is small, and the credential reaches a remote — after which it has to
be rotated whether or not the commit is reverted.

## Decision

**A repository-wide secret scan runs as a unit test**
(`tests/unit/test_no_secrets_committed.py`), and CI additionally runs
`gitleaks` over the commit history.

Four checks:

1. **Provider-shaped keys** — AWS access key ids, private key blocks, GitHub
   and Slack tokens, Google API keys, Stripe keys, JWTs. These have no benign
   form; if one matches, it is real.
2. **Credential-shaped assignments** judged against a placeholder list. An
   assignment whose value contains a recognised marker - `change_me`,
   `placeholder`, `not_a_real`, `${...}`, `os.environ` - is documentation.
   One whose value is an arbitrary high-entropy string is an incident.
3. **DSNs with inline passwords** — the most common accidental leak, because
   it looks like a URL rather than like a secret.
4. **`.env` is gitignored AND absent from the working tree.** Both halves
   matter: an ignored file that exists is one `git add -f` away.

Plus two checks on the safe-local-development path the specification requires:
`.env.example` must declare every required setting, and every credential value
in it must visibly read as a placeholder — because a plausible-looking
password in a template gets copied verbatim into production.

Two decisions about scope are deliberate:

- **The scan has no carve-out for `tests/`.** A credential in a fixture is
  still a credential. The redaction test's fixture values were renamed to
  `placeholder_not_a_real_password` rather than exempting the directory.
- **The scanner asserts it actually walked the repository.** A secret scanner
  with a broken path is indistinguishable from a clean tree, and that is the
  most common way this kind of test becomes decorative.

## Alternatives rejected

**A pre-commit hook only.** Hooks are per-machine and skippable with
`--no-verify`. Useful as a first line, not as the guarantee.

**gitleaks only, in CI.** Correct for history, but it does not run when a
developer runs `make test`, and it does not check the two `.env.example`
properties that are specific to this project's configuration contract.

**Entropy-based detection.** Produces false positives on hashes, UUIDs and
base64 test fixtures — of which this repository has many, including 64-character
SHA-256 row hashes in seed files. A scanner people learn to ignore is worse
than none.

## Consequences

- `make test` fails on a leaked credential, in seconds, before a commit exists.
- The scanner was verified against a planted secret: an AWS key id, a
  non-placeholder password and a DSN with an inline password were dropped into
  `configs/`, and two of the four checks failed as intended. Removing the file
  returned the suite to green.
- The placeholder list is a maintenance surface. It is small, listed in one
  place with the reason for each entry, and a false positive fails loudly
  rather than silently passing.
- It bit this document. An earlier draft of the paragraph above illustrated
  the rule with a literal example of a plausible password, and the scan failed
  on this ADR. That is the correct outcome - the check cannot tell a worked
  example from a leak, and a scanner with an exemption for "files that are
  explaining the scanner" is a scanner with an exemption. The prose was
  rewritten; the rule was not.
