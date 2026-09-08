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

## Deploying the nginx vhost

Two things to know before copying it onto the host.

**It claims `default_server` on ports 80 and 8000, and the stock Debian site at
`/etc/nginx/sites-enabled/default` claims the same.** Both enabled at once is a
duplicate-default-server error and nginx will not start. Remove the stock symlink in the
same step:

```sh
rm /etc/nginx/sites-enabled/default
nginx -t && systemctl reload nginx
```

**`nginx -t` and `nginx -T` need root here** — the vhost reads
`/etc/ssl/private/mast-ns-control.key`, which the `mast` account cannot open, so a
non-root test reports a spurious `[emerg] cannot load certificate key`.

## Reloading Prometheus

`prometheus.yml` is picked up by a SIGHUP, with no scrape gap and no restart:

```sh
promtool check config /etc/prometheus/prometheus.yml
systemctl reload prometheus
curl -s localhost:9090/api/v1/targets?state=active | jq '.data.activeTargets[].labels'
```
