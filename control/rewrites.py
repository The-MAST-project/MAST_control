import socket
from urllib.parse import urlparse, urlunparse

import psutil


def get_external_ipv4():
    """Get all external IPv4 addresses using psutil"""
    ips = []

    for interface, addrs in psutil.net_if_addrs().items():
        for addr in addrs:
            # Check if it's IPv4
            if addr.family == socket.AF_INET:
                ip = addr.address
                # Skip loopback
                if not ip.startswith("127."):
                    ips.append(ip)

    return ips


external_ip = get_external_ipv4()[0]


#
# URL rewrite functions for use when behind a reverse proxy.
# Each function rewrites a direct URL to use the proxy base path and port.
# The proxy base path is indicated by the X-Proxy-Base header,
# and the proxy port is indicated by the X-Proxy-Port header.
#
def rewrite_mast_backend_url(request, direct_url: str) -> str:
    return _rewrite_url(request, direct_url)


def rewrite_wao_safety_url(request, direct_url: str) -> str:
    return _rewrite_url(request, direct_url)


def rewrite_mast_netdata_url(request, direct_url: str) -> str:
    return _rewrite_url(request, direct_url)


def rewrite_mast_share_url(request, direct_url: str) -> str:
    return _rewrite_url(request, direct_url)


def _rewrite_url(request, direct_url):
    """
    Helper to rewrite direct_url to use the proxy base and port if running behind proxy.
    Only rewrites if both X-Proxy-Port and X-Proxy-Base headers are present.
    """
    proxy_port = request.headers.get("X-Proxy-Port")
    proxy_base_hdr = request.headers.get("X-Proxy-Base")
    if not proxy_port or not proxy_base_hdr:
        return direct_url

    scheme = request.url.scheme
    parsed = urlparse(direct_url)
    netloc = f"{external_ip}:{proxy_port}"
    path = proxy_base_hdr.rstrip("/") + parsed.path
    return urlunparse((scheme, netloc, path, "", "", ""))
