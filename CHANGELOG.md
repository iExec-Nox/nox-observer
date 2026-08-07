# Changelog

## 0.1.0 (2026-08-07)


### ⚠ BREAKING CHANGES

* Remove unresolved handles endpoint ([#22](https://github.com/iExec-Nox/nox-observer/issues/22))

### Features

* Add grace period and ignored handles to unresolved count ([#18](https://github.com/iExec-Nox/nox-observer/issues/18)) ([78768f3](https://github.com/iExec-Nox/nox-observer/commit/78768f33cf9f62f6229ff8221ac95496fa8935c4))
* Add PostgreSQL support with initial schema and Docker configuration ([#1](https://github.com/iExec-Nox/nox-observer/issues/1)) ([f99a6c1](https://github.com/iExec-Nox/nox-observer/commit/f99a6c1d6021ccd8ea4278606ad29336014f50fd))
* add PostgreSQL TLS configuration support ([#12](https://github.com/iExec-Nox/nox-observer/issues/12)) ([5ddb35e](https://github.com/iExec-Nox/nox-observer/commit/5ddb35e4036a2891cbea2e229900187cf9598daa))
* implement block cursor pagination for subgraph polling ([#15](https://github.com/iExec-Nox/nox-observer/issues/15)) ([b5743ca](https://github.com/iExec-Nox/nox-observer/commit/b5743ca3edbf8f7e87d72045344d4a763e4808be))
* implement multichain support ([#11](https://github.com/iExec-Nox/nox-observer/issues/11)) ([1903fc3](https://github.com/iExec-Nox/nox-observer/commit/1903fc3f3ce84e7313858e64c49e92e129d4bb5f))
* Implement unresolved handles endpoint - part1 ([#16](https://github.com/iExec-Nox/nox-observer/issues/16)) ([46116c3](https://github.com/iExec-Nox/nox-observer/commit/46116c3054125995668ab2e92377ccda99813814))
* initialize basic project structure ([#2](https://github.com/iExec-Nox/nox-observer/issues/2)) ([7f67f43](https://github.com/iExec-Nox/nox-observer/commit/7f67f4318309bbcbca779c2d61a3f810790ccf4e))
* **nats:** add consumer foundation ([#7](https://github.com/iExec-Nox/nox-observer/issues/7)) ([ff0427a](https://github.com/iExec-Nox/nox-observer/commit/ff0427aef5c8971bcec57cfb06ecd70a61bbc79d))
* **nats:** add JetStream connection client ([#8](https://github.com/iExec-Nox/nox-observer/issues/8)) ([b362415](https://github.com/iExec-Nox/nox-observer/commit/b3624152b1d2724e3765d252e4af6a40073a3391))
* **nats:** wire JetStream consumer pull loop into application lifecycle ([#9](https://github.com/iExec-Nox/nox-observer/issues/9)) ([d2c0bf2](https://github.com/iExec-Nox/nox-observer/commit/d2c0bf24a0d90549fa73d32058e2ae1fbad7ddf8))
* Remove unresolved handles endpoint ([#22](https://github.com/iExec-Nox/nox-observer/issues/22)) ([e099f47](https://github.com/iExec-Nox/nox-observer/commit/e099f475e01090d2fd7e2e8fae09440f89a61f8a))
* s3 tune concurrency ([#13](https://github.com/iExec-Nox/nox-observer/issues/13)) ([604b2de](https://github.com/iExec-Nox/nox-observer/commit/604b2dedb2d212b76f8174832ba532e1debb0089))
* **s3:** implement S3 resolver ([#10](https://github.com/iExec-Nox/nox-observer/issues/10)) ([28e4009](https://github.com/iExec-Nox/nox-observer/commit/28e4009343417843d1557217850810563932dcdd))
* **subgraph:** introduce typed GraphQL client and configuration scaffolding (1/2) ([#4](https://github.com/iExec-Nox/nox-observer/issues/4)) ([bcf6462](https://github.com/iExec-Nox/nox-observer/commit/bcf646208d6d73efbb794afe1cc5e427d92cb981))
* **subgraph:** poll handles with catch-up and live mode (2/2) ([#5](https://github.com/iExec-Nox/nox-observer/issues/5)) ([1822ba6](https://github.com/iExec-Nox/nox-observer/commit/1822ba617825465987b194ed242745d8dcc39942))


### Bug Fixes

* enhance database configuration with structured URL components ([#14](https://github.com/iExec-Nox/nox-observer/issues/14)) ([fc976e2](https://github.com/iExec-Nox/nox-observer/commit/fc976e201571d1a78e358a0774ae4ac43138381e))
