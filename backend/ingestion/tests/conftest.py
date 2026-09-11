"""Shared fixtures for the ingestion test suite."""

import ipaddress
import socket

import pytest


@pytest.fixture(autouse=True)
def _public_dns_for_tests(monkeypatch):
    """Hermetic DNS: hostnames resolve to a public IP in tests.

    XIN-62 put an SSRF gate (with live DNS resolution) at the service
    boundary. Real DNS answers are environment-dependent (and intercepted in
    some sandboxes), which would make the example.com-based tests flaky for
    non-code reasons. Fake hostname resolution to a public IP; literal IPs
    pass through to the real resolver so SSRF-rejection tests stay meaningful.
    """
    real_getaddrinfo = socket.getaddrinfo

    def fake(host, port, *args, **kwargs):
        try:
            ipaddress.ip_address(host)  # literal IP -> real resolution
            return real_getaddrinfo(host, port, *args, **kwargs)
        except ValueError:
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 0))]

    monkeypatch.setattr(socket, "getaddrinfo", fake)
