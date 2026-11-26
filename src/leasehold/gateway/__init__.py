"""The browser gateway: a local CONNECT proxy between a sandboxed browser and the
registry exits. It alone holds the exit tokens and applies the per-lease host policy
(``leasehold.gateway.policy``) to every tunnel (``leasehold.gateway.server``)."""
