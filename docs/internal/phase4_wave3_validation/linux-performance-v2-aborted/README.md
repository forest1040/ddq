# Resource-aborted Linux performance campaign v2

Only buffered/P1 was started. The small workload completed two warmups and seven measured pairs. The 120-layer mixed workload completed one warmup pair (645.157 s off, 670.131 s on); nine pairs would take about 11838 s, exceeding the declared 3600 s command budget. The container was deliberately stopped before any measured long-workload pair. The enclosing driver exited 137. No subsequent configuration started.

All recorded rows and the source inventory are retained. There is no final campaign summary or accepted performance conclusion. `audit.json` treats this progress-only artifact as inconclusive, including the completed small slice; extraction time is nested in capture time and is not added twice. Empty observed backend arrays do not prove amplitude O_DIRECT I/O. The next independent campaign fixes long layers at 20 before measurement and preserves warmup 2 / paired measured 7; it will use fresh scratch and output directories.
