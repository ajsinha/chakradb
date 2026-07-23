#!/usr/bin/env bash
# Build the ChakraDB book to LaTeX (.tex) and PDF from the mdBook Markdown.
#
# Pipeline:  Markdown (docs/book/src, ordered by SUMMARY.md)
#              → pandoc (+ Lua filter renders ```mermaid to PDF figures)
#              → chakradb-documentation.tex
#              → tectonic → chakradb-documentation.pdf
#
# Self-contained tools (no root needed) live in ~/.local/book-tools:
#   pandoc, tectonic, and node_modules/.bin/mmdc (mermaid-cli).
# Mermaid renders via the system Chrome (see puppeteer.json).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BOOK="$ROOT/docs/book"
SRC="$BOOK/src"
LATEXDIR="$BOOK/latex"
OUT="$BOOK/build"
TOOLS="${BOOK_TOOLS:-$HOME/.local/book-tools}"

export PATH="$TOOLS:$PATH"
export PUPPETEER_SKIP_DOWNLOAD=true
export MMDC="$TOOLS/node_modules/.bin/mmdc"
export DIAGRAM_DIR="diagrams"

for tool in "$TOOLS/pandoc" "$TOOLS/tectonic" "$MMDC"; do
  [ -x "$tool" ] || { echo "missing tool: $tool"; echo "see scripts/README-book.md"; exit 1; }
done

mkdir -p "$OUT"
cat > "$OUT/puppeteer.json" <<'JSON'
{ "executablePath": "/usr/bin/google-chrome",
  "args": ["--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage"] }
JSON
export PUPPETEER_CONFIG="$OUT/puppeteer.json"

echo "==> assembling chapters in SUMMARY order"
python3 - "$SRC" "$OUT/book.md" "$LATEXDIR/metadata.yaml" <<'PY'
import re, sys, os
src, out, meta = sys.argv[1], sys.argv[2], sys.argv[3]
lines = open(os.path.join(src, "SUMMARY.md")).read().splitlines()
with open(out, "w") as o:
    o.write(open(meta).read())          # YAML front matter (title, preamble)
    o.write("\n\n")
    for line in lines:
        h = re.match(r'^#\s+(.*)', line)
        if h:
            name = h.group(1).strip()
            if name.lower() == "summary":
                continue
            name = re.sub(r'^Part\s+[IVXLC]+\s+[—-]\s+', '', name)  # "Part I — X" -> "X"
            # Escape LaTeX specials in the part title (e.g. "&").
            for a, b in [('\\', r'\textbackslash{}'), ('&', r'\&'), ('%', r'\%'),
                         ('#', r'\#'), ('_', r'\_'), ('$', r'\$')]:
                name = name.replace(a, b)
            o.write("\n```{=latex}\n\\part{%s}\n```\n\n" % name)
            continue
        link = re.search(r'\]\(([A-Za-z0-9/_.-]+\.md)\)', line)
        if link:
            p = link.group(1)
            if p == "title.md":
                continue
            fp = os.path.join(src, p)
            if os.path.isfile(fp):
                o.write(open(fp).read())
                o.write("\n\n")
print("assembled", out)
PY

echo "==> pandoc → LaTeX (rendering mermaid diagrams; first run is slower)"
( cd "$OUT" && "$TOOLS/pandoc" book.md \
    --from=markdown \
    --lua-filter="$LATEXDIR/mermaid.lua" \
    --include-in-header="$LATEXDIR/preamble.tex" \
    --include-before-body="$LATEXDIR/titlepage.tex" \
    --metadata title-meta="ChakraDB — The Definitive Guide" \
    --top-level-division=chapter \
    --standalone \
    --output=chakradb-documentation.tex )

echo "==> tectonic → PDF (first run fetches LaTeX packages)"
( cd "$OUT" && "$TOOLS/tectonic" --keep-logs --synctex=0 chakradb-documentation.tex )

echo "==> done:"
ls -la "$OUT/chakradb-documentation.tex" "$OUT/chakradb-documentation.pdf" 2>/dev/null
