# vi: se terse nows sts=4 sw=4 ts=4:
.PHONY: help deploy-control-services

SRC := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

help:
	@echo "Targets:"
	@echo "  deploy-control-services  Copy and enable user systemd services for MAST_control"

deploy-control-services:
	cp $(SRC).config/systemd/user/mast-control.service $(HOME)/.config/systemd/user/
	systemctl --user daemon-reload
	systemctl --user enable --now mast-control
	@echo "mast-control service deployed and started"
	@systemctl --user status mast-control --no-pager
