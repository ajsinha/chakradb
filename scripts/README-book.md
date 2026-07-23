# Building the ChakraDB book (HTML, LaTeX & PDF)

The book source is an [mdBook](https://rust-lang.github.io/mdBook/) under
`docs/book/`. It renders two ways:

- **HTML** (browsable site) via `mdbook`.
- **LaTeX (`.tex`) + PDF** via `scripts/build-book.sh` (pandoc → LaTeX → tectonic),
  with Mermaid diagrams pre-rendered to PDF figures. This produces
  `docs/book/build/chakradb-documentation.{tex,pdf}`.

## One-time tool setup (no root required)

The PDF pipeline uses three self-contained tools plus the system Chrome. Install
them under `~/.local/book-tools`:

```bash
mkdir -p ~/.local/book-tools && cd ~/.local/book-tools

# pandoc (static binary)
curl -sSL https://github.com/jgm/pandoc/releases/download/3.5/pandoc-3.5-linux-amd64.tar.gz \
  | tar xz && cp pandoc-3.5/bin/pandoc .

# tectonic (single-binary LaTeX engine — fetches only the packages it needs)
curl -sSL "https://github.com/tectonic-typesetting/tectonic/releases/download/tectonic%400.15.0/tectonic-0.15.0-x86_64-unknown-linux-musl.tar.gz" \
  | tar xz

# mermaid-cli (renders ```mermaid blocks; uses the system google-chrome)
PUPPETEER_SKIP_DOWNLOAD=true npm install @mermaid-js/mermaid-cli@11
```

Requires: `curl`, `node`/`npm`, and `google-chrome` (or set `executablePath` in the
`puppeteer.json` the build writes). tectonic downloads LaTeX packages on first run
and caches them.

## Build the PDF + LaTeX

```bash
bash scripts/build-book.sh
# → docs/book/build/chakradb-documentation.pdf
# → docs/book/build/chakradb-documentation.tex   (+ diagrams/)
```

The `.tex` is standalone: with the rendered `diagrams/` alongside it, any XeLaTeX
engine (`xelatex`, or `tectonic chakradb-documentation.tex`) reproduces the PDF.

## Build the HTML site

```bash
cargo install mdbook mdbook-mermaid   # once
mdbook-mermaid install docs/book       # adds mermaid.min.js
mdbook build docs/book                 # → docs/book/book/
mdbook serve docs/book                 # live preview
```

## How the pieces fit

| File | Role |
|---|---|
| `docs/book/src/SUMMARY.md` | table of contents (chapter order) |
| `docs/book/src/**/*.md` | chapters (Markdown + ```mermaid diagrams) |
| `docs/book/book.toml` | mdBook config (HTML) |
| `docs/book/latex/preamble.tex` | LaTeX preamble (fonts, colors, headers) |
| `docs/book/latex/titlepage.tex` | the colored cover (TikZ chakra emblem) |
| `docs/book/latex/metadata.yaml` | pandoc document settings |
| `docs/book/latex/mermaid.lua` | pandoc filter: ```mermaid → PDF figure |
| `scripts/build-book.sh` | assemble → pandoc → tectonic |

Generated output (`docs/book/build/`) is git-ignored; rebuild it with the script.
