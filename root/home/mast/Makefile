# vi: se terse nows sts=4 sw=4 ts=4:
.PHONY: help deploy-control-services make-certs deploy-certs deploy-nginx deploy-prometheus check-deployed

SRC        := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
REPO_ROOT  := $(abspath $(SRC)../../..)
HOSTNAME   := mast-ns-control
FQDN       := mast-ns-control.weizmann.ac.il
IP         := 10.23.1.181
CERT_DIR   := $(REPO_ROOT)/root/etc/ssl/certs
CA_DIR     := $(REPO_ROOT)/root/usr/local/share/ca-certificates
KEY_DIR    := /etc/ssl/private
NGINX_CONF := $(REPO_ROOT)/root/etc/nginx/conf.d/$(HOSTNAME).conf
PROM_CONF  := $(REPO_ROOT)/root/etc/prometheus/prometheus.yml

# Every file this tree owns, as <path under root/>:<path on the host>. The list
# is what check-deployed walks; a file added to root/ without an entry here is
# invisible to the drift check, so add both together.
DEPLOYED := \
    etc/nginx/conf.d/$(HOSTNAME).conf:/etc/nginx/conf.d/$(HOSTNAME).conf \
    etc/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml \
    etc/ssl/certs/$(HOSTNAME).crt:/etc/ssl/certs/$(HOSTNAME).crt \
    usr/local/share/ca-certificates/mast-local-ca.crt:/usr/local/share/ca-certificates/mast-local-ca.crt \
    home/mast/.config/systemd/user/mast-control.service:$(HOME)/.config/systemd/user/mast-control.service

# Divergences that are known and deliberately not failures. mast-control.service
# is absent from mast-ns-control, and that host is not this work's to change:
# whether the control service belongs there, and in what form, is its owner's
# call. Reported on every run so it stays visible, but it does not fail the
# check -- a drift check that is always red is a drift check nobody reads.
TOLERATED := $(HOME)/.config/systemd/user/mast-control.service

help:
	@echo "Targets:"
	@echo "  deploy-control-services  Copy and enable user systemd services for MAST_control"
	@echo "  make-certs               Generate local CA and server cert (outputs to repo + /tmp)"
	@echo "  deploy-certs             Install certs into /etc/ssl and reload nginx (needs root)"
	@echo "  deploy-nginx             Install the nginx vhost, test it, reload (needs root)"
	@echo "  deploy-prometheus        Install prometheus.yml and SIGHUP prometheus (needs root)"
	@echo "  check-deployed           Diff every tracked file against the host (no root)"

make-certs:
	# Both are git-tracked directories, but git does not track empty ones: removing the
	# retired mast-wis-control.crt took $(CERT_DIR) with it, and openssl will not create
	# an output directory itself.
	mkdir -p $(CERT_DIR) $(CA_DIR)
	@echo "Generating local CA..."
	openssl genrsa -out /tmp/mast-ca.key 4096
	openssl req -x509 -new -nodes -key /tmp/mast-ca.key -sha256 -days 3650 \
	    -out $(CA_DIR)/mast-local-ca.crt \
	    -subj "/CN=MAST Local CA/O=Weizmann MAST"
	@echo "Generating server cert..."
	openssl genrsa -out /tmp/$(HOSTNAME).key 2048
	openssl req -new -key /tmp/$(HOSTNAME).key \
	    -out /tmp/$(HOSTNAME).csr \
	    -subj "/CN=$(FQDN)"
	# Written to a file rather than passed via `<(...)`: make runs recipes under
	# /bin/sh, which is dash on Debian/Ubuntu, and process substitution is a bashism.
	# It failed with: /bin/sh: Syntax error: "(" unexpected
	printf "subjectAltName=DNS:$(FQDN),DNS:$(HOSTNAME),IP:$(IP)\n" > /tmp/$(HOSTNAME).ext
	openssl x509 -req -in /tmp/$(HOSTNAME).csr \
	    -CA $(CA_DIR)/mast-local-ca.crt -CAkey /tmp/mast-ca.key -CAcreateserial \
	    -out $(CERT_DIR)/$(HOSTNAME).crt -days 3650 -sha256 \
	    -extfile /tmp/$(HOSTNAME).ext
	@openssl verify -CAfile $(CA_DIR)/mast-local-ca.crt $(CERT_DIR)/$(HOSTNAME).crt
	@echo "Keys in /tmp (not committed). Certs in repo. Run 'make deploy-certs' (as root) to install."

deploy-certs:
	@[ "$(shell id -u)" = "0" ] || { echo "Error: deploy-certs must be run as root"; exit 1; }
	cp $(CERT_DIR)/$(HOSTNAME).crt /etc/ssl/certs/$(HOSTNAME).crt
	cp /tmp/$(HOSTNAME).key $(KEY_DIR)/$(HOSTNAME).key
	chmod 600 $(KEY_DIR)/$(HOSTNAME).key
	cp $(CA_DIR)/mast-local-ca.crt /usr/local/share/ca-certificates/mast-local-ca.crt
	update-ca-certificates
	# Reloaded so the new certificate takes effect. The vhost is NOT copied here:
	# this target reads /tmp/$(HOSTNAME).key, which exists only just after
	# make-certs, so a config-only change routed through it died on the missing
	# key before ever reaching the copy. Use deploy-nginx for the vhost.
	nginx -t && systemctl reload nginx
	@echo "Certs deployed and nginx reloaded."

# Install the nginx vhost from the repo.
#
# Refuses while the stock Debian site is enabled: this vhost claims
# default_server on ports 80 and 8000 so that a request by IP, or under an
# unexpected name, is redirected rather than falling through to that site's 404.
# Two default servers on one port is a startup error, so the two cannot both be
# enabled -- and nginx failing to start takes the whole control host's web
# surface with it.
#
# The live file is kept as .bak across the test (nginx includes conf.d/*.conf,
# which .bak does not match) and put back if nginx -t fails, so a bad config
# never survives the target that installed it.
deploy-nginx:
	@[ "$(shell id -u)" = "0" ] || { echo "Error: deploy-nginx must be run as root"; exit 1; }
	@[ ! -e /etc/nginx/sites-enabled/default ] || { \
	    echo "Error: the stock Debian site is enabled and claims default_server on port 80,"; \
	    echo "       which this vhost also claims. nginx would refuse to start. Remove it:"; \
	    echo "         rm /etc/nginx/sites-enabled/default"; \
	    exit 1; }
	@[ -f $(NGINX_CONF) ] || { echo "Error: $(NGINX_CONF) not found"; exit 1; }
	-cp -f /etc/nginx/conf.d/$(HOSTNAME).conf /etc/nginx/conf.d/$(HOSTNAME).conf.bak
	cp $(NGINX_CONF) /etc/nginx/conf.d/$(HOSTNAME).conf
	@if nginx -t; then \
	    systemctl reload nginx; \
	    echo "nginx vhost deployed and reloaded."; \
	else \
	    echo "nginx -t failed; restoring the previous vhost, nginx left running as it was."; \
	    [ -f /etc/nginx/conf.d/$(HOSTNAME).conf.bak ] && mv /etc/nginx/conf.d/$(HOSTNAME).conf.bak /etc/nginx/conf.d/$(HOSTNAME).conf; \
	    exit 1; \
	fi

# Install prometheus.yml from the repo.
#
# Checked before it is copied, so a bad file never reaches /etc. Reloaded with
# SIGHUP rather than `systemctl reload`: this unit declares no ExecReload
# (CanReload=no) and prometheus is started without --web.enable-lifecycle, so
# POST /-/reload answers 403. SIGHUP is the only reload this host accepts, and
# it costs no scrape gap.
#
# SIGHUP is asynchronous and returns immediately, so the reload is CONFIRMED
# rather than assumed: this waits for
# prometheus_config_last_reload_success_timestamp_seconds to pass the moment the
# signal was sent, and only then prints the targets. Sleeping instead printed
# whatever was loaded a moment earlier -- on the first real deploy that meant
# the OLD target list under the words "deployed and reloaded", which is the one
# thing a deploy tool must not do. A reload that does not confirm within 20s
# fails the target: prometheus goes on serving the previous config, and saying
# so is the entire point.
deploy-prometheus:
	@[ "$(shell id -u)" = "0" ] || { echo "Error: deploy-prometheus must be run as root"; exit 1; }
	promtool check config $(PROM_CONF)
	cp $(PROM_CONF) /etc/prometheus/prometheus.yml
	chown prometheus:prometheus /etc/prometheus/prometheus.yml
	chmod 644 /etc/prometheus/prometheus.yml
	@t0=$$(date +%s); \
	systemctl kill -s HUP prometheus; \
	ok=0; i=0; \
	while [ $$i -lt 20 ]; do \
	    ts=$$(curl -s --noproxy '*' \
	        'http://127.0.0.1:9090/api/v1/query?query=prometheus_config_last_reload_success_timestamp_seconds' \
	        | tr ',' '\n' | grep -o '"[0-9][0-9.]*"' | tail -1 | tr -d '"'); \
	    case "$$ts" in [0-9]*) [ "$${ts%%.*}" -ge "$$t0" ] && { ok=1; break; } ;; esac; \
	    i=$$((i+1)); sleep 1; \
	done; \
	if [ $$ok -eq 1 ]; then \
	    echo "prometheus reloaded the new config. Active targets:"; \
	else \
	    echo "prometheus did NOT confirm a successful config reload within 20s -- it is"; \
	    echo "still running the previous config. Check: journalctl -u prometheus -n 20"; \
	fi; \
	curl -s --noproxy '*' 'http://127.0.0.1:9090/api/v1/targets?state=active' \
	    | tr ',' '\n' | grep -o '"instance":"[^"]*"' | sort -u; \
	[ $$ok -eq 1 ]

deploy-control-services:
	cp $(SRC).config/systemd/user/mast-control.service $(HOME)/.config/systemd/user/
	systemctl --user daemon-reload
	systemctl --user enable --now mast-control
	@echo "mast-control service deployed and started"
	@systemctl --user status mast-control --no-pager

# Is the host still what this repo says it is?
#
# The repo is authoritative for the files it carries, but nothing enforces
# that: no auto-deploy, no CI, and /etc stays writable. This is the check that
# turns the convention into something answerable -- run it and a hand edit made
# on the box, or a merged change nobody deployed, stops being invisible.
#
# Reads only, and needs no root: every tracked file is world-readable. Exits
# non-zero on any difference, so it also works as a cron line or a CI step on a
# runner that can reach the host.
check-deployed:
	@rc=0; \
	for pair in $(DEPLOYED); do \
	    src=$(REPO_ROOT)/root/$${pair%%:*}; \
	    dst=$${pair#*:}; \
	    case " $(TOLERATED) " in *" $$dst "*) tol=1 ;; *) tol=0 ;; esac; \
	    if [ ! -e "$$dst" ]; then \
	        if [ $$tol -eq 1 ]; then printf 'tolerated %s (absent; not this repo to place)\n' "$$dst"; \
	        else printf 'MISSING  %s\n' "$$dst"; rc=1; fi; \
	    elif diff -q "$$src" "$$dst" >/dev/null 2>&1; then \
	        printf 'ok       %s\n' "$$dst"; \
	    elif [ $$tol -eq 1 ]; then \
	        printf 'tolerated %s (differs; not this repo to change)\n' "$$dst"; \
	    else \
	        printf 'DRIFTED  %s\n' "$$dst"; \
	        diff -u "$$dst" "$$src" 2>&1 | sed 's/^/         /'; \
	        rc=1; \
	    fi; \
	done; \
	if [ $$rc -eq 0 ]; then \
	    echo "The host matches the repo, tolerated entries aside."; \
	else \
	    echo "Differences above are - host, + repo. Deploy the repo, or bring the host's change back into it."; \
	fi; \
	exit $$rc
