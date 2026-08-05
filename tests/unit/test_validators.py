"""L1-20 .. L1-26 — SSRF and header validation.

A job payload naming a URL the server will later fetch is the one genuine
injection vector in this system, so its rules get their own matrix.
"""

import pytest

from app.core.enums import JobType
from app.core.errors import PayloadValidationError
from app.jobs.registry import get_job_class


def parse_webhook(url: str, **extra):
    return get_job_class(JobType.WEBHOOK).parse_payload({"url": url, **extra})


@pytest.mark.parametrize(
    "url",
    [
        "https://example.com/hook",
        "http://example.com/hook",
        "https://api.example.co.uk:8443/a/b?c=d",
        "https://8.8.8.8/hook",
    ],
)
def test_l1_20_public_urls_are_accepted(url):
    assert parse_webhook(url)


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1/x",
        "http://127.1.2.3/x",
        "http://[::1]/x",
        "http://0.0.0.0/x",
    ],
)
def test_l1_21_loopback_is_rejected(url):
    with pytest.raises(PayloadValidationError):
        parse_webhook(url)


@pytest.mark.parametrize(
    "url",
    [
        "http://10.0.0.1/x",
        "http://192.168.1.1/x",
        "http://172.16.0.1/x",
        "http://[fd00::1]/x",
    ],
)
def test_l1_22_private_ranges_are_rejected(url):
    with pytest.raises(PayloadValidationError):
        parse_webhook(url)


@pytest.mark.parametrize(
    "url",
    [
        "http://169.254.169.254/latest/meta-data/",
        "http://[fe80::1]/x",
        "http://[::ffff:127.0.0.1]/x",
    ],
)
def test_l1_23_link_local_and_metadata_endpoints_are_rejected(url):
    """169.254.169.254 is the cloud instance metadata endpoint — the classic
    SSRF target for stealing instance credentials."""
    with pytest.raises(PayloadValidationError):
        parse_webhook(url)


@pytest.mark.parametrize(
    "url",
    [
        "http://localhost/x",
        "http://LOCALHOST/x",
        "http://api.localhost/x",
        "http://service.local/x",
        "http://db.internal/x",
        "http://localhost./x",
    ],
)
def test_l1_24_internal_hostnames_are_rejected(url):
    with pytest.raises(PayloadValidationError):
        parse_webhook(url)


@pytest.mark.parametrize(
    "url",
    ["file:///etc/passwd", "gopher://example.com/x", "ftp://example.com/x", "ws://example.com"],
)
def test_l1_25_non_http_schemes_are_rejected(url):
    with pytest.raises(PayloadValidationError):
        parse_webhook(url)


@pytest.mark.parametrize("url", ["http://", "notaurl", "https://"])
def test_l1_26_malformed_urls_are_rejected(url):
    with pytest.raises(PayloadValidationError):
        parse_webhook(url)


@pytest.mark.parametrize("url", ["http://intranet/x", "https:///path", "http://db"])
def test_l1_26b_single_label_hostnames_are_rejected(url):
    """A name with no dot resolves through the local search domain to an
    internal host. Note that pydantic normalises "https:///path" to the host
    "path", which is exactly such a name."""
    with pytest.raises(PayloadValidationError):
        parse_webhook(url)


def test_headers_are_bounded():
    assert parse_webhook("https://example.com", headers={"X-Trace": "abc"})

    with pytest.raises(PayloadValidationError):
        parse_webhook("https://example.com", headers={f"H{i}": "v" for i in range(11)})
    with pytest.raises(PayloadValidationError):
        parse_webhook("https://example.com", headers={"X" * 65: "v"})
    with pytest.raises(PayloadValidationError):
        parse_webhook("https://example.com", headers={"X-Big": "v" * 1025})
    with pytest.raises(PayloadValidationError):
        parse_webhook("https://example.com", headers={"Bad:Name": "v"})
    with pytest.raises(PayloadValidationError):
        parse_webhook("https://example.com", headers={"": "v"})
