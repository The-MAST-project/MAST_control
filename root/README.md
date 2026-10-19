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

How-to: [Control machine services — Check the host against the repo](https://github.com/The-MAST-project/MAST/wiki/Control-machine-services#check-the-host-against-the-repo)

## Deploying

How-to: [Control machine services — Deploy nginx or Prometheus config](https://github.com/The-MAST-project/MAST/wiki/Control-machine-services#deploy-nginx-or-prometheus-config)

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
