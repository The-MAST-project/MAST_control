# `root/` — a mirror of the control host's system configuration

Files here shadow their real paths on `mast-ns-control`:
`root/etc/nginx/conf.d/mast-ns-control.conf` is `/etc/nginx/conf.d/mast-ns-control.conf`
on the host, and so on.

## Direction of truth

**This tree is authoritative for the files it contains.** Its content was verified
against the live host before any of it was edited, and `root/home/mast/Makefile` puts it
back: `deploy-nginx`, `deploy-prometheus`, `deploy-certs`, `deploy-control-services`. The
working order is repo → pull request → `make deploy-*` on the host, and `/etc` on
`mast-ns-control` is not edited by hand.

**Little enforces that.** There is no auto-deploy on merge and no CI, and `/etc` stays
writable, so the deploy remains a manual step someone runs on the host as root. What
there is, is a way to *ask*: `make check-deployed` (below) answers whether the host is
still what this tree says, so a hand edit on the box — or a merged change nobody
deployed — is no longer invisible.

**The host stays authoritative for anything not in this tree** — `grafana.ini`, the
systemd units this repo does not ship, whatever `sites-enabled/` currently holds.
Bringing one of those under version control means capturing it verified-identical first,
before any edit, the way `prometheus.yml` was.

Every file was compared against the live host on 2026-09-08 before being touched. The
nginx vhost and the certificate matched byte-for-byte; `prometheus.yml` was absent here
and was committed unmodified before any edit, so each later change to it is a diff
against what the host was actually running.

## What is deliberately not here

- **Private keys.** `/etc/ssl/private/*` stays on the host; `make-certs` writes keys to
  `/tmp` and commits only the certificate. That split is the rule, not an accident.
- **`/etc/grafana/grafana.ini`** — `root:grafana` `0640`, unreadable by the `mast`
  account, 90 KB of largely stock template, and the natural home for an admin password.
  Mirroring it needs a privileged read and a scrub pass; neither has been done.
- Anything else carrying a credential.

## Checking for drift

```sh
cd <checkout>/root/home/mast
make check-deployed
```

Diffs every file this tree owns against its live counterpart and exits non-zero on any
difference, so it works as a cron line or a CI step as readily as by hand. It reads only
and **needs no root** — every tracked file is world-readable. Output is one line per
file, `ok` / `DRIFTED` / `MISSING`, with a unified diff under each drifted one, read as
`-` host, `+` repo.

A `tolerated` line is a divergence that is known and deliberately not a failure. The
Makefile's `TOLERATED` variable holds them, currently just
`mast-control.service`, which is absent from `mast-ns-control`: whether the control
service belongs on that host, and in what form, is its owner's call and not this repo's.
It is printed on every run so it stays visible, and it does not turn the check red — a
drift check that is always red is one nobody reads.

Two things it does not do. It knows only the files listed in the Makefile's `DEPLOYED`
variable, so a file added under `root/` without an entry there is invisible to it — add
both together. And it reports; it never reconciles. A `DRIFTED` line is a question with
two legitimate answers: deploy the repo, or bring the host's change back into the repo
and review it like any other.

## Deploying

Both targets run **on the host**, as root, from a checkout there
(`/home/mast/PycharmProjects/MAST_control`), from `root/home/mast/`. Nothing here
reaches out over the network — there is no `ssh` or `scp` in the Makefile.

```sh
cd <checkout>/root/home/mast
sudo make deploy-nginx        # vhost -> /etc/nginx/conf.d, nginx -t, reload
sudo make deploy-prometheus   # promtool check, prometheus.yml -> /etc, SIGHUP
```

`deploy-nginx` refuses while `/etc/nginx/sites-enabled/default` exists. That stock
Debian site claims `default_server` on port 80 and so does this vhost, and two of them
is a startup error — nginx failing to start takes the control host's whole web surface
with it. Remove the symlink and re-run:

```sh
rm /etc/nginx/sites-enabled/default
```

The live vhost is kept as `.bak` across the test and restored if `nginx -t` fails, so a
bad config cannot survive the target that installed it. `.bak` is not matched by the
`conf.d/*.conf` include, so the backup is inert.

`deploy-prometheus` reloads with **SIGHUP**, not `systemctl reload`: the unit declares no
`ExecReload` (`CanReload=no`), and prometheus runs without `--web.enable-lifecycle`, so
`POST /-/reload` answers 403. SIGHUP costs no scrape gap. The config is checked with
`promtool` *before* it is copied, so a bad file never reaches `/etc`.

**`nginx -t` and `nginx -T` need root here** — the vhost reads
`/etc/ssl/private/mast-ns-control.key`, which the `mast` account cannot open, so a
non-root test reports a spurious `[emerg] cannot load certificate key`.

`deploy-certs` installs certificates only. It used to copy the vhost as well, which made
a config-only change impossible to ship through it: the target reads
`/tmp/<host>.key`, a file that exists only just after `make-certs`, and died on the
missing key before reaching the copy.

## What is established, and what is not

The work this tree came out of was scoped to **Grafana and the metrics behind it**. Much
was measured on the host along the way; a fair amount was not, and the two are worth
keeping apart.

**Established, by measurement on `mast-ns-control` (2026-09-08):**

- Grafana and Prometheus on this host are MAST's own monitoring stack — Grafana on `:3000`
  served from the `/grafana/` sub-path, one Prometheus datasource, `localhost:9090`,
  default. The Grafana at `10.23.1.25:3000` is the LAST observatory's and carries the
  weather/safety dashboard `mast_safety` links; the two exporter dashboards `MAST_gui`
  links are relative paths served from here.
- `mast@weizmann.ac.il` authenticates against this Grafana at viewer level, not admin.
- The Windows Exporter dashboard (`IV0hu1m7z`) selects a host with the template variable
  **`server`**, whose values are instances like `mast01:9182`.
- windows_exporter answers on the units — mast01 and mast03 sampled directly — and on
  `mast-ns-spec`, which is Windows and uses the same port despite the name.
- Prometheus here reloads on **SIGHUP only**: the unit declares no `ExecReload`, and the
  process runs without `--web.enable-lifecycle`, so `POST /-/reload` answers 403.
- The nginx vhost and both certificates in this tree match the host byte-for-byte.

**Not established:**

- **Whether the dashboards render the fleet's data correctly** once it is scraped. Nothing
  has been checked past the scrape config. One known complication: the `windows-servers`
  relabel writes a `hostname` label that collides with one the exporter already publishes,
  so `windows_os_hostname` comes back carrying both `hostname` (ours) and
  `exported_hostname` (its own). Whether the relabel is needed at all is open.
- **Which Windows dashboard is meant to be the live one.** There are two — the 22-panel
  `IV0hu1m7z` that `MAST_gui` links, and a 27-panel "Windows Exporter Dashboard 2024"
  keyed on `job`/`hostname`/`instance` whose variables have never been populated.
- **The GUI.** `/mast-dash/` and `/mast-backend/` proxy to 8010 and 8002, both of which
  are down, so those locations answer 502. Whether they ever served here, what owns them,
  and when they last ran are all unknown — deliberately out of scope, and their nginx
  locations were left exactly as found.
- **`mast-control.service`**, per the tolerated entry above.
- **`grafana.ini`.** Unreadable by the `mast` account and never inspected.
- **Whether the units trust the local CA**, which decides whether an operator browsing to
  `https://mast-ns-control.weizmann.ac.il` from a unit sees a warning. Untested, and the
  reason a unit-side Grafana shortcut is better aimed at `http://mast-ns-control:3000/`
  directly.
- **That any of this works on the host.** Nothing in this tree has been deployed; the two
  `DRIFTED` lines are exactly that.
