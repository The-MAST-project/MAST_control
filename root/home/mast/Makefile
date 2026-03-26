# vi: se terse nows sts=4 sw=4 ts=4:
.PHONY: help deploy-control-services make-certs deploy-certs

SRC        := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
REPO_ROOT  := $(abspath $(SRC)../../..)
HOSTNAME   := mast-wis-control
FQDN       := mast-wis-control.weizmann.ac.il
IP         := 10.23.3.73
CERT_DIR   := $(REPO_ROOT)/root/etc/ssl/certs
CA_DIR     := $(REPO_ROOT)/root/usr/local/share/ca-certificates
KEY_DIR    := /etc/ssl/private

help:
	@echo "Targets:"
	@echo "  deploy-control-services  Copy and enable user systemd services for MAST_control"
	@echo "  make-certs               Generate local CA and server cert (outputs to repo + /tmp)"
	@echo "  deploy-certs             Install certs into /etc/ssl and reload nginx (needs root)"

make-certs:
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
	openssl x509 -req -in /tmp/$(HOSTNAME).csr \
	    -CA $(CA_DIR)/mast-local-ca.crt -CAkey /tmp/mast-ca.key -CAcreateserial \
	    -out $(CERT_DIR)/$(HOSTNAME).crt -days 3650 -sha256 \
	    -extfile <(printf "subjectAltName=DNS:$(FQDN),DNS:$(HOSTNAME),IP:$(IP)")
	@openssl verify -CAfile $(CA_DIR)/mast-local-ca.crt $(CERT_DIR)/$(HOSTNAME).crt
	@echo "Keys in /tmp (not committed). Certs in repo. Run 'make deploy-certs' (as root) to install."

deploy-certs:
	@[ "$(shell id -u)" = "0" ] || { echo "Error: deploy-certs must be run as root"; exit 1; }
	cp $(CERT_DIR)/$(HOSTNAME).crt /etc/ssl/certs/$(HOSTNAME).crt
	cp /tmp/$(HOSTNAME).key $(KEY_DIR)/$(HOSTNAME).key
	chmod 600 $(KEY_DIR)/$(HOSTNAME).key
	cp $(CA_DIR)/mast-local-ca.crt /usr/local/share/ca-certificates/mast-local-ca.crt
	update-ca-certificates
	cp $(REPO_ROOT)/root/etc/nginx/conf.d/mast-wis-control.conf /etc/nginx/conf.d/
	nginx -t && systemctl reload nginx
	@echo "Certs deployed and nginx reloaded."

deploy-control-services:
	cp $(SRC).config/systemd/user/mast-control.service $(HOME)/.config/systemd/user/
	systemctl --user daemon-reload
	systemctl --user enable --now mast-control
	@echo "mast-control service deployed and started"
	@systemctl --user status mast-control --no-pager
