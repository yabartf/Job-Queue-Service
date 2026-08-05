"""Payload field validators shared across job types."""

import ipaddress
from typing import Annotated

from pydantic import AfterValidator, HttpUrl

#: Suffixes conventionally used for names that resolve inside a network.
_BLOCKED_SUFFIXES = (".localhost", ".local", ".internal")


def reject_internal_url(url: HttpUrl) -> HttpUrl:
    """Reject URLs that point back into infrastructure rather than out of it.

    A job payload naming a URL the server will later fetch is the one genuine
    injection vector in this system (SSRF). ``HttpUrl`` already constrains the
    scheme to http/https; this adds the host checks.

    Known limitation: this cannot be complete at validation time. A hostname
    that resolves publicly now can resolve to 127.0.0.1 by the time the request
    is made (DNS rebinding), and a queue's submit-to-execute gap is exactly
    where that is exploitable. Closing it requires resolving the host at request
    time and pinning the address in the HTTP client. The webhook here is
    simulated and issues no real request, so the residual risk is nil today.
    """
    host = url.host
    if not host:  # pragma: no cover - HttpUrl already requires a host
        raise ValueError("URL must include a host")

    lowered = host.lower().rstrip(".")
    # Pydantic keeps IPv6 literals bracketed, exactly as they appear in the URL.
    if lowered.startswith("[") and lowered.endswith("]"):
        lowered = lowered[1:-1]

    if lowered.endswith(_BLOCKED_SUFFIXES):
        raise ValueError("URL host is not publicly routable")

    try:
        address = ipaddress.ip_address(lowered)
    except ValueError:
        # A name, not a literal. A single-label name such as "localhost" or
        # "intranet" resolves through the local search domain to an internal
        # host, so it is treated as internal; every public name has a dot.
        if "." not in lowered:
            raise ValueError("URL host is not publicly routable") from None
        # Resolution of a dotted name happens at request time; see above.
        return url

    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
        address = address.ipv4_mapped

    # is_global excludes private, loopback, link-local (169.254.169.254 — the
    # cloud instance metadata endpoint), multicast and reserved ranges.
    if not address.is_global:
        raise ValueError("URL host is not publicly routable")

    return url


#: An http(s) URL that is not pointed at our own infrastructure.
SafeHttpUrl = Annotated[HttpUrl, AfterValidator(reject_internal_url)]


def validate_headers(headers: dict[str, str]) -> dict[str, str]:
    """Bound the size and shape of caller-supplied HTTP headers."""
    for name, value in headers.items():
        if not name or not all(c.isascii() and c.isprintable() and c != ":" for c in name):
            raise ValueError(f"Invalid header name: {name!r}")
        if len(name) > 64:
            raise ValueError(f"Header name too long: {name!r}")
        if len(value) > 1024:
            raise ValueError(f"Header value too long for {name!r}")
    return headers
