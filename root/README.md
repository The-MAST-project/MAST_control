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

**Nothing enforces that.** No auto-deploy on merge, no CI check, no drift detection: an
edit made directly on the host diverges silently, and the deploy is a manual step someone
runs there as root. What holds the convention up is that the hand edit is now the worse
path — it gets no review, and the next `make deploy-*` overwrites it without noticing.

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
