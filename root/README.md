# `root/` — a mirror of the control host's system configuration

Files here shadow their real paths on `mast-ns-control`:
`root/etc/nginx/conf.d/mast-ns-control.conf` is `/etc/nginx/conf.d/mast-ns-control.conf`
on the host, and so on.

## Direction of truth

**Today the host is authoritative and this tree is a duplicate**, captured by hand.
Nothing deploys from here on its own, and a file changed here has no effect on a running
host until someone copies it across. The duplicate earns its place by making the
configuration reviewable: a change arrives as a diff in a pull request instead of as an
edit on a box whose history no one can reconstruct.

**The intended end state is the reverse** — the repo authoritative, applied after a PR
merges. `root/home/mast/Makefile` is the seed of it: `deploy-certs` and
`deploy-control-services` already push from the repo onto the filesystem.

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
