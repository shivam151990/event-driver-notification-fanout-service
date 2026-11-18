# DigitalOcean deployment

The repository includes a container image definition and an App Platform specification. These are deployment assets, not a claim that a live deployment exists. Provisioning and deployment need your DigitalOcean account, billing, a connected GitHub repository, database credentials, and chosen region.

The target is **App Platform + managed PostgreSQL + managed Kafka**. Local Docker volumes are not used in production. The service can run multiple instances: database locks coordinate publication, row leases coordinate delivery, and a Kafka consumer group coordinates fanout.

## Provision the backing services

1. Create a managed PostgreSQL cluster with high availability and backups. Create database `fanout` and an application user. Flyway needs schema migration permissions on startup; for stricter separation, run migrations with a deployment role and give the runtime role only table/sequence access.
2. Create a managed Kafka cluster in the selected region, with enough brokers for replication factor 3. Create the `fanout.events.v1` topic with at least 3 partitions, replication factor 3, and `min.insync.replicas=2`. Set retention to exceed the longest supported fanout outage; PostgreSQL remains the durable event/outbox record. Disable unclean leader election.
3. Give the application Kafka user permission to write/read this topic and use consumer group `fanout-v1`. This service intentionally does not create topics during startup.
4. Download each cluster's CA certificate and Kafka SASL credentials. Use the **SASL connection port**, not the mutual-TLS port. The `cloud` profile uses SASL/SCRAM-SHA-256 over verified TLS with a PEM trust anchor. Hostname verification stays enabled.
5. Configure trusted sources/network access for the App Platform app. Use VPC connectivity where supported for your chosen products and region. Verify both database and Kafka routes from the deployed app; a successful local connection does not establish cloud connectivity.

The official guides describe [managed Kafka connections](https://docs.digitalocean.com/products/databases/kafka/how-to/connect/) and [App Platform database integration](https://docs.digitalocean.com/products/app-platform/how-to/manage-databases/).

## Configure and deploy

1. Push this project to your GitHub repository and connect that repository to DigitalOcean.
2. Copy `deploy/digitalocean/app.yaml` to a private, untracked location. Replace the repository and connection placeholders, confirm the region and instance size, and enter runtime secrets through the DigitalOcean control panel or private spec.
3. Set `POSTGRES_CA_CERT` and `KAFKA_CA_CERT` to the **actual multiline PEM text**. In YAML, use a block scalar (`value: |`) with indented certificate lines. Do not put backslash-escaped newlines in these values.
4. Set the JDBC URL to the managed PostgreSQL host/database. Keep `sslmode=verify-full&sslrootcert=/tmp/postgres-ca.crt`. The non-root container entrypoint writes the injected PostgreSQL certificate to that path with restrictive permissions. Certificates never need to be copied into the image.
5. Set `KAFKA_SASL_JAAS_CONFIG` to a complete JAAS string using your assigned credentials. Escape quotes/backslashes in credentials according to Kafka JAAS syntax. Do not expose passwords in workflow logs.
6. Generate `FANOUT_API_KEY` with at least 32 random characters. Configure the app's secrets as `SECRET`/`RUN_TIME`. Leave HTTP/private webhook exceptions disabled.
7. Install and authenticate `doctl`, then validate and submit the completed spec:

```sh
doctl auth init
doctl apps spec validate /path/to/private-app.yaml
doctl apps create --spec /path/to/private-app.yaml --wait
```

App Platform terminates public HTTPS and forwards traffic to container port 8080. The template uses two app instances and a readiness check. Database migrations coordinate through Flyway's lock. Back up the database and use backward-compatible migrations before rolling out future releases.

The [App Spec reference](https://docs.digitalocean.com/products/app-platform/reference/app-spec/) documents the submitted configuration and available component fields.

## Verify the deployed service

```sh
curl --fail https://YOUR-APP-DOMAIN/actuator/health/readiness
curl --fail https://YOUR-APP-DOMAIN/api/v1/subscriptions \
  -H "X-API-Key: $FANOUT_API_KEY"
```

Register a real HTTPS webhook, ingest an event using a unique idempotency key, and inspect its delivery and attempt records. Verify the receiver checks the HMAC signature and deduplicates `X-Delivery-Id`. Exercise a controlled `503` response, observe the retry/audit, then restore `2xx`. Confirm metrics and alerts, managed-database backups, and a restore drill before accepting production traffic.

If readiness passes but events remain unprocessed, check Kafka topic existence, SASL port/credentials, certificate chain, permissions, consumer lag, and outbox backlog. Readiness checks the database; Kafka outages are deliberately buffered by the outbox and need their own alerts.

If deployment fails before Java starts with `/app/entrypoint.sh: permission denied`, set the web service's Run Command to `/bin/sh /app/entrypoint.sh` and redeploy. This runs the existing certificate setup before starting Java. The Dockerfile explicitly sets script permissions and invokes `/bin/sh`; publish that Dockerfile to the connected GitHub branch and rebuild for the permanent image fix. The local workspace alone does not update App Platform's GitHub source. After deploying the fixed image, the Run Command override can be cleared. A generic “Container Terminated” banner alone does not establish an out-of-memory failure; use the detailed deployment logs.

## GitHub deployment workflow

The push/PR workflow runs unit and real-container integration tests and builds the Docker image. To enable manual deployment:

1. Create a GitHub environment named `production`, configure any required reviewers, and add its `DIGITALOCEAN_ACCESS_TOKEN` secret with app deployment permissions.
2. Ensure the App Platform app points to this repository's `main` branch, with automatic deployment disabled as in the supplied spec.
3. Run **Deploy to DigitalOcean** on `main` and enter the existing app ID. It reruns the full suite and then asks App Platform to build/deploy the configured branch.

This manual pipeline deploys the current remote `main` head. Avoid concurrent pushes during a production deployment; for immutable release promotion, harden it to publish a digest-pinned image to a registry and deploy that tested digest. The template does not automatically provision billable resources, push secrets to GitHub, or deploy on every source push.

## Recovery and scaling

- Kafka outages accumulate durable outbox work; restore connectivity and monitor catch-up. Do not delete unpublished outbox rows.
- Database loss requires managed backup/PITR recovery. After restore, consumers may have offsets ahead of restored fanout state. The outbox reconciler republishes retained events still awaiting fanout after five minutes, but it cannot recreate events absent from the restored database. Validate restored state and duplicates in a reviewed recovery procedure before resuming traffic.
- Increase app instances for independent source streams, but test database connection capacity and subscriber rate limits. The relay remains deliberately serialized; measure it before planning large-volume workloads.
- Keep source partition count stable when relying on Kafka source ordering. The database ordering gate protects dispatch, but partition changes and replay deserve a controlled rollout and load test.
- Restrict outbound access with egress controls in addition to application URL validation. Add ingress rate limiting and tenant authorization before exposing this administrative API to untrusted clients.
