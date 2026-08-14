# Design-documentation tooling for design/current/.
#
#   make d2           render every .d2 diagram source to a sibling .svg
#   make update-docs  have Claude bring these documents back in line with
#                     the current code, adding d2 diagrams where they help

REPO_ROOT := $(abspath ../..)

D2_SRCS := $(shell find . -name '*.d2')
D2_SVGS := $(D2_SRCS:.d2=.svg)

.PHONY: d2 update-docs all

all: d2 update-docs

# One d2 invocation per diagram. `d2 in.d2 [out.svg]` takes a SINGLE input
# and treats any second path as the OUTPUT, so `d2 a.d2 b.d2` would render
# a.d2 and overwrite b.d2 with the result -- destroying a diagram source.
# The pattern rule also makes rendering incremental: only diagrams whose
# source changed are re-rendered.
d2: $(D2_SVGS)

%.svg: %.d2
	@which d2 >/dev/null 2>&1 || curl -fsSL https://d2lang.com/install.sh | sh -s --
	d2 $< $@

# Prompt for `make update-docs`. Kept here rather than in a separate file so
# the target is self-contained; `export` + "$$UPDATE_DOCS_PROMPT" passes it
# through the shell without quoting hazards (the shell does not rescan an
# expanded variable's value, so backticks in the prompt are safe).
define UPDATE_DOCS_PROMPT
Update the design documentation in design/current/ of this Rust Kafka client
so that it describes the CURRENT code.

Read every markdown document in design/current/, then verify their claims
against the code in src/ and the conventions in CLAUDE.md and .claude/rules/.
Correct whatever has drifted: renamed types, moved modules, mechanisms that
were replaced by another, wrong counts, dead file paths. Cite file:line for
structural claims so the next reader can re-verify them.

Never mention project phases or milestones in these documents. They describe
the code as it stands, so process artifacts -- "Milestone 8", "Tier 3 Phase
7", "landed in Phase 41", "Phase 12.5 round-2" -- date the prose and tell a
reader nothing about how the client works. Where such a reference does carry
information, restate it as the behaviour or the reason for it: write "the
background loop drives reconciliation once per iteration, so a listener
callback cannot stall heartbeats" rather than "Phase 41 made reconciliation
non-blocking". The phase-by-phase record belongs in design/history/
(MILESTONES.md and the per-milestone directories); do not copy it back here.

Two limits on that rule. First, it targets PROJECT process artifacts, not
names that exist in the code: the consumer background-task loop really does
label its steps "Phase 1 ... Phase 2.4 ..." in run_once
(src/consumer/internals/consumer_network_thread.rs), mirroring Java's
ConsumerNetworkThread, and those labels stay -- they point at code the reader
can go and find. Second, status.md is exempt: tracking what is done is its
job and it is organised by phase, so leave its structure and its phase and
milestone headings alone. Cross-references to it from the other documents are
fine, but drop the phase number from the pointer -- "see status.md", not "see
status.md Tier 3 Phase 7".

Do not invent history. Several of these documents record a past
investigation or root-cause analysis; keep their findings and note plainly
what has changed since, rather than rewriting the conclusion as if the
original analysis had said something else. If a document points at a file or
directory that no longer exists, say so instead of silently dropping the
reference.

Add diagrams wherever a diagram carries the design better than prose does --
component and ownership structure, the consumer background-task loop and its
phases, the producer send path, the event and threading model, the error
type hierarchy. Write each diagram as a d2 source file in
design/current/img/ (for example img/consumer-threading.d2), then run
`make -C design/current d2` to render it to a sibling .svg in the same
folder, and embed that .svg in the relevant markdown with a normal image
link plus a one-line caption stating what the reader should take away from
it. Keep each diagram small enough to read at a glance: prefer several
focused diagrams over one exhaustive one. The .d2 file is the source of
truth -- never hand-edit a .svg.

Every image lives in design/current/img/ -- the .d2 sources, the rendered
.svg files, and any other picture -- so the markdown documents stay at the
top of design/current/ and links into the folder read
`![Caption](img/name.svg)`. If you find a diagram source or an image still
sitting at the top level, move it into img/ and fix the links that point at
it. Do not invent a second layout (no diagrams/, no assets/, no per-document
subfolders). The `make -C design/current d2` target renders every .d2 it
finds recursively, so a new diagram needs no Makefile change; check that the
.svg appeared next to its source before embedding it.

Constraints: only create or modify files under design/current/. Do not touch
code, tests, CLAUDE.md or the rule files. Preserve each document's existing
structure and headings unless a section is genuinely wrong. Finish by
reporting what you changed, which diagrams you added, and anything you could
not verify against the code.
endef
export UPDATE_DOCS_PROMPT

# Runs from the repo root so Claude sees the whole tree and picks up the
# project CLAUDE.md; the prompt confines its writes to design/current/.
# acceptEdits auto-approves the doc edits, and the tool allow-list is the
# minimum the prompt needs -- notably NOT unrestricted Bash.
update-docs:
	@command -v claude >/dev/null 2>&1 || { echo "claude CLI not found on PATH"; exit 1; }
	cd $(REPO_ROOT) && claude -p "$$UPDATE_DOCS_PROMPT" \
		--permission-mode auto \
		--output-format stream-json \
		--verbose --model opus \
		--settings '{"showThinkingSummaries": true}' \
		--print \
		--allowedTools "Read Glob Grep Edit Write Bash(make *) Bash(git log *)"
