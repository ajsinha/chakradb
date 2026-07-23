-- Pandoc Lua filter: render ```mermaid fenced blocks to PDF figures for the
-- LaTeX / PDF build. In the HTML (mdBook) build the blocks are left as-is and
-- rendered by mdbook-mermaid; this filter only runs in the pandoc pipeline.
--
-- Environment:
--   MMDC              path to the mermaid-cli binary (mmdc)
--   PUPPETEER_CONFIG  puppeteer.json (points at the system Chrome)
--   DIAGRAM_DIR       output directory for the rendered .pdf diagrams

local count = 0
local mmdc = os.getenv("MMDC") or "mmdc"
local pconf = os.getenv("PUPPETEER_CONFIG") or "puppeteer.json"
local outdir = os.getenv("DIAGRAM_DIR") or "diagrams"

function CodeBlock(block)
  if not block.classes:includes("mermaid") then
    return nil
  end
  count = count + 1
  os.execute("mkdir -p '" .. outdir .. "'")
  local base = outdir .. "/diagram-" .. count
  local mmd = base .. ".mmd"
  local pdf = base .. ".pdf"

  local fh = io.open(mmd, "w")
  fh:write(block.text)
  fh:close()

  local cmd = string.format(
    "%s -p '%s' -i '%s' -o '%s' -b transparent >/dev/null 2>&1",
    mmdc, pconf, mmd, pdf
  )
  local ok = os.execute(cmd)
  os.remove(mmd)

  if ok then
    -- Centered, bounded in both dimensions, aspect preserved.
    return pandoc.RawBlock(
      "latex",
      "\\begin{center}\\includegraphics[width=0.92\\linewidth," ..
      "height=0.80\\textheight,keepaspectratio]{" .. pdf .. "}\\end{center}"
    )
  else
    -- Fall back to showing the diagram source rather than dropping it.
    return block
  end
end
