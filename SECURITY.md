# Security policy

## Reporting a vulnerability

Please do not open a public issue for a security problem.

- Use GitHub's private vulnerability reporting: **Security → Report a
  vulnerability** on this repository. The report reaches the maintainers only.
- Aerospike customers can also raise it through their usual Aerospike support
  channel.

Include the client version, the server version, the feature set you build
with, and the steps to reproduce. You will get an acknowledgement, and a fix
or mitigation is coordinated with you before anything is published.

## Supported versions

Security fixes are released on the current major line of the `aerospike`
crate. Older lines receive them on a best-effort basis; upgrade to the latest
release to be sure of getting them.

## Scope

The crates published from this repository: `aerospike`, `aerospike-core`,
`aerospike-sync`, `aerospike-rt` and `aerospike-macro`. Problems in the
Aerospike server belong to the server's own process, not to this client.
