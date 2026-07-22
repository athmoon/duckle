#!/usr/bin/env python3
"""Render the terminal demos used on the PyPI page.

Self-contained SVG: no asciinema, no recorder, no runtime dependency.

The transcript is ALWAYS fully visible and only the cursor blinks. A
progressive line-by-line reveal was built first and rejected: embedded
through an <img> tag, which is exactly how PyPI and GitHub render it, the
animation was repeatedly observed sitting on its first frame, leaving a
terminal that showed one line and nothing else. Content a reader needs must
never depend on an animation actually running, least of all on the hero
image of a package page. The cursor blink is CSS rather than script because
GitHub serves raw SVG under
`Content-Security-Policy: default-src 'none'; style-src 'unsafe-inline'`,
so inline style survives where script would not.

Every line below is real captured output, not a mock-up.

    python packaging/pypi/make_demo_svg.py
"""

import os

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.abspath(os.path.join(HERE, "..", "..", "docs", "assets"))

# Duckle palette. Success reads maya, never green (brand rule).
BG = "#0f1720"
CHROME = "#18222e"
FG = "#c9d5e1"
DIM = "#7d8ea1"
LEMON = "#f5d90a"
MAYA = "#5bc8f5"
ORANGE = "#f59f0a"
RED = "#f2555a"

CHAR_W = 8.4
LINE_H = 21
PAD_X = 18
TOP = 44


def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def render(path, title, lines):
    """lines: list of (kind, text); kind picks the row colour.

    `title` is the window chrome caption. It should describe what the reader
    is looking at, not repeat the command, which is already the first line of
    the transcript: a title bar reading "uvx duckle quickstart" directly above
    "$ uvx duckle quickstart" just looks like a rendering bug.
    """
    width = max(660, int(PAD_X * 2 + CHAR_W * max(len(t) for _, t in lines) + 10))
    height = TOP + LINE_H * (len(lines) + 1) + 16

    body = []
    for i, (kind, text) in enumerate(lines):
        y = TOP + LINE_H * (i + 1)
        if kind == "cmd":
            body.append(
                '<text x="{x}" y="{y}" class="m">'
                '<tspan class="p">$</tspan> <tspan class="c">{t}</tspan></text>'.format(
                    x=PAD_X, y=y, t=esc(text)
                )
            )
        elif text:
            body.append(
                '<text x="{x}" y="{y}" class="m {k}">{t}</text>'.format(
                    x=PAD_X, y=y, k=kind, t=esc(text).replace(" ", "&#160;")
                )
            )

    cursor_y = TOP + LINE_H * (len(lines) + 1) - 11
    svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" '
        'viewBox="0 0 {w} {h}" role="img" aria-label="{title}">\n'
        "<style>\n"
        ".m{{font-family:'SFMono-Regular',Consolas,'Liberation Mono',Menlo,monospace;"
        "font-size:13.5px;white-space:pre}}\n"
        ".p{{fill:{lemon};font-weight:700}}\n"
        ".c{{fill:{fg};font-weight:600}}\n"
        ".out{{fill:{fg}}}\n"
        ".dim{{fill:{dim}}}\n"
        ".ok{{fill:{maya}}}\n"
        ".err{{fill:{red}}}\n"
        ".key{{fill:{lemon}}}\n"
        ".cur{{animation:blink 1.06s steps(1,end) infinite}}\n"
        "@keyframes blink{{0%,50%{{opacity:1}}50.01%,100%{{opacity:0}}}}\n"
        "</style>\n"
        '<rect width="{w}" height="{h}" rx="9" fill="{bg}"/>\n'
        '<rect width="{w}" height="30" rx="9" fill="{chrome}"/>\n'
        '<rect y="21" width="{w}" height="9" fill="{chrome}"/>\n'
        '<circle cx="19" cy="15" r="5" fill="{red}"/>\n'
        '<circle cx="37" cy="15" r="5" fill="{orange}"/>\n'
        '<circle cx="55" cy="15" r="5" fill="{maya}"/>\n'
        '<text x="{tx}" y="19.5" class="m dim" font-size="11.5">{title}</text>\n'
        "{body}\n"
        '<rect class="cur" x="{cx}" y="{cy}" width="8" height="15" fill="{lemon}"/>\n'
        "</svg>\n"
    ).format(
        w=width, h=height, title=esc(title), body="\n".join(body),
        bg=BG, chrome=CHROME, fg=FG, dim=DIM, lemon=LEMON, maya=MAYA,
        orange=ORANGE, red=RED, tx=max(70, width // 2 - len(title) * 3),
        cx=PAD_X, cy=cursor_y,
    )
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(svg)
    print("wrote {} ({:.1f} KB, {}x{})".format(
        os.path.basename(path), len(svg) / 1024, width, height))


# --------------------------------------------------------------- demo 1
# The onboarding prompt, not a terminal session. A first-time user pastes one
# sentence into their agent and the agent does the rest. Everything the agent
# prints is verbatim from a real `uvx duckle quickstart` run.
INSTALL = [
    ("key", "> Run uvx duckle quickstart to build my first pipeline and run it"),
    ("out", ""),
    ("dim", "  Running uvx duckle quickstart ..."),
    ("out", ""),
    ("out", "  Duckle quickstart"),
    ("out", ""),
    ("dim", "    created  orders.csv  (8 rows of sample data)"),
    ("dim", "    created  pipelines/quickstart.json"),
    ("out", ""),
    ("out", "    status   : ok"),
    ("dim", "    duration : 212 ms"),
    ("ok",  "      csv        ok (8 rows)"),
    ("ok",  "      filter     ok (5 rows)"),
    ("ok",  "      derive     ok (5 rows)"),
    ("ok",  "      out        ok (5 rows)"),
    ("out", ""),
    ("dim", "    id,region,customer,amount,total,tag"),
    ("out", "    2,EU,Globex,25,30.0,EU-Globex"),
    ("out", "    3,US,Initech,40,48.0,US-Initech"),
    ("out", "    4,UK,Umbrella,30,36.0,UK-Umbrella"),
    ("out", "    6,EU,Soylent,55,66.0,EU-Soylent"),
    ("out", "    7,UK,Vehement,22,26.4,UK-Vehement"),
    ("out", ""),
    ("out", "  Your first pipeline ran: 8 rows in, 5 out, written to"),
    ("out", "  out.csv. Nothing was installed."),
]

# --------------------------------------------------------------- demo 2
# The CI gate: compile-checks with no engine, no credentials, no network.
VALIDATE = [
    ("cmd", "duckle validate"),
    ("err", "FAIL  pipelines/broken.json"),
    ("dim", "      config: Filter (xf.filter / filter): missing main input"),
    ("ok",  "ok    pipelines/orders.json  (4 stages)"),
    ("out", ""),
    ("out", "2 pipeline(s) checked, 1 failed"),
    ("out", ""),
    ("cmd", "echo $?"),
    ("key", "1"),
    ("out", ""),
    ("dim", "# 0 clean | 1 a real finding | 2 the runner could not start"),
    ("dim", "# No DuckDB, no credentials, no network: validate only compiles."),
]


# --------------------------------------------------------------- demo 3
# The agent loop. Opens with the one command a first-time user types, then
# follows the conversation. Tool results are verbatim from a real MCP session.
AGENT = [
    ("cmd", "claude mcp add duckle -- uvx duckle mcp"),
    ("ok",  "Added stdio MCP server duckle"),
    ("out", ""),
    ("cmd", "claude"),
    ("out", ""),
    ("key", "> load orders.csv into parquet, keep orders over 20"),
    ("out", ""),
    ("dim", "  duckle - get_component_schema(\"xf.filter\")"),
    ("out", "      predicate, rejectOnError"),
    ("out", ""),
    ("dim", "  duckle - create_pipeline(\"big-orders\")"),
    ("ok",  "      ok   3 stages compiled"),
    ("out", ""),
    ("dim", "  duckle - run_pipeline(\"big-orders.json\")"),
    ("ok",  "      ok   169 ms"),
    ("ok",  "        csv      ok (4 rows)"),
    ("ok",  "        filter   ok (3 rows)"),
    ("ok",  "        parquet  ok (3 rows)"),
    ("out", ""),
    ("out", "  Wrote big_orders.parquet. The pipeline is saved as JSON,"),
    ("out", "  so you can open it on the canvas and see what I built."),
    ("out", ""),
    ("dim", "# Compile-checked before it ran. No pip install, no engine setup."),
]


# --------------------------------------------------------------- demo 4
# The pip path. Where uvx is for trying it, pip is for keeping it: a
# persistent `duckle` command, `duckle-mcp`, and `import duckle` in Python.
# Captured verbatim from a fresh install of 0.5.9 off PyPI.
PIP = [
    ("cmd", "pip install duckle"),
    ("dim", "Downloading duckle-0.5.9-py3-none-win_amd64.whl (23.0 MB)"),
    ("dim", "Downloading duckdb_cli-1.5.4-py3-none-win_amd64.whl (12.9 MB)"),
    ("ok",  "Successfully installed duckdb-cli-1.5.4 duckle-0.5.9"),
    ("out", ""),
    ("dim", "# three commands, and the engine, on your PATH:"),
    ("dim", "#   duckle   duckle-mcp   duckdb"),
    ("out", ""),
    ("cmd", "cat job.py"),
    ("key", "import duckle"),
    ("key", "from duckle import col"),
    ("out", ""),
    ("out", '(duckle.read_csv("orders.csv")'),
    ("out", "    .where(col.amount >= 20)"),
    ("out", '    .derive(total="round(amount * 1.2, 2)")'),
    ("out", '    .write_parquet("out.parquet")'),
    ("out", "    .run())"),
    ("out", ""),
    ("cmd", "python job.py"),
    ("out", "status   : ok"),
    ("dim", "duration : 388 ms"),
    ("ok",  "  csv        ok (5 rows)"),
    ("ok",  "  filter     ok (3 rows)"),
    ("ok",  "  pyexpr     ok (3 rows)"),
    ("ok",  "  parquet    ok (3 rows)"),
    ("out", ""),
    ("dim", "# Python built the plan. DuckDB moved the rows."),
]


# --------------------------------------------------------------- demo 5
# MySQL in Docker -> DuckDB at 1M rows. Every number below is measured:
# mysql:8.0 holding 1,000,000 orders, 333,333 of them shipped.
MYSQL_DUCKDB = [
    ("cmd", "docker ps"),
    ("dim", "duckle-demo-mysql   mysql:8.0   Up   0.0.0.0:3306->3306/tcp"),
    ("out", ""),
    ("cmd", "mysql -e 'SELECT count(*) FROM orders_1m'"),
    ("key", "1000000"),
    ("out", ""),
    ("cmd", "cat pipeline.py"),
    ("key", "import duckle"),
    ("key", "from duckle import col"),
    ("out", ""),
    ("out", "(duckle.src.mysql(host=\"127.0.0.1\", database=\"ducktest\","),
    ("out", "                  password=\"${ENV:MYSQL_PASSWORD}\","),
    ("out", "                  tableName=\"orders_1m\")"),
    ("out", "    .where(col.status == \"shipped\")"),
    ("out", "    .derive(net=\"round(amount * 0.8, 2)\","),
    ("out", "            label=\"f'{region}-{customer}'\")"),
    ("out", "    .snk.duckdb(database=\"warehouse.duckdb\","),
    ("out", "                tableName=\"shipped_orders\")"),
    ("out", "    .run())"),
    ("out", ""),
    ("cmd", "python pipeline.py"),
    ("out", "status   : ok"),
    ("key", "duration : 1932 ms"),
    ("ok",  "  mysql      ok"),
    ("ok",  "  filter     ok (333333 rows)"),
    ("ok",  "  pyexpr     ok (333333 rows)"),
    ("ok",  "  duckdb     ok (333333 rows)"),
    ("out", ""),
    ("dim", "# 1,000,000 rows scanned, 333,333 landed, in under two seconds."),
    ("dim", "# ~518k source rows/sec, MySQL wire to DuckDB table."),
    ("dim", "# No row touched Python: the filter and both derived columns"),
    ("dim", "# were compiled to SQL and run inside DuckDB."),
]

# --------------------------------------------------------------- demo 6
# Same source, Salesforce sink. Compile-checked for real: validate opens no
# source and writes no sink, so it needs no MySQL and no Salesforce org.
MYSQL_SALESFORCE = [
    ("dim", "# same pipeline, one line changed: where it lands"),
    ("out", ""),
    ("out", "(duckle.src.mysql(host=\"127.0.0.1\", database=\"ducktest\","),
    ("out", "                  tableName=\"orders\")"),
    ("out", "    .where(col.status == \"shipped\")"),
    ("out", "    .derive(Name=\"f'{region}-{customer}'\", Amount__c=\"amount\")"),
    ("key", "    .snk.salesforce(object=\"Order__c\", operation=\"upsert\","),
    ("key", "                    externalIdField=\"Ext_Id__c\","),
    ("key", "                    accessToken=\"${ENV:SF_TOKEN}\"))"),
    ("out", ""),
    ("cmd", "duckle validate pipelines/to_salesforce.json"),
    ("ok",  "ok    pipelines/to_salesforce.json  (4 stages)"),
    ("out", ""),
    ("out", "1 pipeline(s) checked, 0 failed"),
    ("out", ""),
    ("dim", "# Compiled without opening MySQL or contacting Salesforce."),
    ("dim", "# No credentials needed to prove the pipeline is sound."),
]


# --------------------------------------------------------------- demo 7
# MySQL -> MongoDB, a full-table migration. Measured against mysql:8.0 and
# mongo:7 in Docker, 1,000,000 rows, verified document counts on the far side.
MYSQL_MONGO = [
    ("cmd", "docker ps"),
    ("dim", "duckle-demo-mysql   mysql:8.0   Up   0.0.0.0:3306->3306/tcp"),
    ("dim", "duckle-demo-mongo   mongo:7     Up   0.0.0.0:27017->27017/tcp"),
    ("out", ""),
    ("cmd", "cat migrate.py"),
    ("key", "import duckle"),
    ("out", ""),
    ("out", "(duckle.src.mysql(host=\"127.0.0.1\", database=\"ducktest\","),
    ("out", "                  password=\"${ENV:MYSQL_PASSWORD}\","),
    ("out", "                  tableName=\"orders_1m\")"),
    ("out", "    .snk.mongodb(uri=\"${ENV:MONGO_URI}\", database=\"warehouse\","),
    ("out", "                 collection=\"orders\", mode=\"overwrite\")"),
    ("out", "    .run())"),
    ("out", ""),
    ("cmd", "python migrate.py"),
    ("out", "status   : ok"),
    ("key", "duration : 30201 ms"),
    ("ok",  "  mysql      ok (1000000 rows)"),
    ("ok",  "  mongodb    ok (1000000 rows)"),
    ("out", ""),
    ("cmd", "mongosh --eval 'db.orders.countDocuments({})'"),
    ("key", "1000000"),
    ("cmd", "mongosh --eval 'db.orders.countDocuments({status:\"shipped\"})'"),
    ("key", "333333"),
    ("out", ""),
    ("dim", "# A million rows migrated in 30 seconds, counts matching on both"),
    ("dim", "# sides. Two lines of Python, no migration tool, no cloud."),
]


if __name__ == "__main__":
    os.makedirs(OUT_DIR, exist_ok=True)
    render(os.path.join(OUT_DIR, "pypi-demo-install.svg"),
           "paste this into Claude Code, Cursor or Codex", INSTALL)
    render(os.path.join(OUT_DIR, "pypi-demo-validate.svg"),
           "the CI gate", VALIDATE)
    render(os.path.join(OUT_DIR, "pypi-demo-agent.svg"),
           "your agent, with real tools", AGENT)
    render(os.path.join(OUT_DIR, "pypi-demo-pip.svg"),
           "pip install duckle", PIP)
    render(os.path.join(OUT_DIR, "demo-mysql-duckdb.svg"),
           "MySQL in Docker to DuckDB", MYSQL_DUCKDB)
    render(os.path.join(OUT_DIR, "demo-mysql-salesforce.svg"),
           "same source, Salesforce sink", MYSQL_SALESFORCE)
    render(os.path.join(OUT_DIR, "demo-mysql-mongodb.svg"),
           "MySQL to MongoDB migration", MYSQL_MONGO)
