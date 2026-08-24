# Remote-access security design gate

Status: required before any non-loopback binding is considered.

Piqo remains loopback-only. This document is a review gate, not an
implementation authorization. A future proposal must define and receive review
for authenticated identities and authorization, TLS termination and certificate
lifecycle, tenant/workspace isolation, rate and abuse controls, reduced remote
permission profiles, audit retention, incident response, and a migration path
that preserves the private sidecar protocol. Until then `validate_bind_address`
rejects every non-loopback address.
