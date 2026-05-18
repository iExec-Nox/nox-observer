# nox-observer

Rust service that observes Nox protocol flows and materializes handle state in Postgres for debugging and alerting.

## Local development

### Prerequisites

- Docker and Docker Compose

### Start Postgres

```bash
cp .env.example .env
docker compose up -d
```

`sql/schema.sql` is loaded automatically the first time the volume is created (mounted into the Postgres init scripts directory).

To reload the schema from scratch:

```bash
docker compose down -v
docker compose up -d
```

### Connect

The compose stack ships a [pgAdmin](https://www.pgadmin.org/) container reachable at
[http://localhost:5050](http://localhost:5050) (default login: `admin@example.com` / `admin`).
Register the Postgres server inside pgAdmin with host `postgres`, port `5432`, and the credentials from your `.env`.

Prefer another client? Point any Postgres GUI at the `DATABASE_URL` from your `.env`
(default: `postgres://nox_user:nox_password@localhost:5432/nox_observer`). A few good ones: [DBeaver](https://dbeaver.io/), [TablePlus](https://tableplus.com/), [Postico](https://eggerapps.at/postico2/).
