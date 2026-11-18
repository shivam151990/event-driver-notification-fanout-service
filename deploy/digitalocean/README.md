# DigitalOcean deployment

Deploy the service as **App Platform + managed PostgreSQL + managed Kafka**. Use this guide to reproduce the deployment in your own DigitalOcean account. The supplied [app specification](app.yaml) contains placeholders; provisioning requires account permissions, billing, a connected GitHub repository, and a chosen region.

PostgreSQL is required at startup for Flyway and durable storage. Kafka is required for the normal event-to-webhook flow. There is no database-free demo profile. App Platform builds the `Dockerfile`; it does not run the PostgreSQL and Kafka containers in `compose.yaml`. To test without buying managed clusters, use the [local Compose setup](../../README.md#start-locally). To test an already deployed app, go to [Postman setup](../../README.md#test-with-postman) and the [DigitalOcean testing guide](../../docs/DigitalOcean-testing-guide.md).

## 1. Provision PostgreSQL and Kafka

1. Create a managed PostgreSQL cluster in the chosen region. PostgreSQL 17 is used by the local and integration-test environments. A single primary is sufficient for an interview demonstration; choose an HA-capable plan and verify backup/restore procedures before production use.
2. Create database `fanout` and an application user, or use an existing database for the demonstration. The database name and username in `DB_URL`/`DB_USERNAME` must match the selected connection details. The example name `fanout` is not created automatically by the application. Flyway creates application tables and requires schema migration permissions. For stricter separation, run migrations with a deployment role and restrict the runtime role to table/sequence access.
3. Create managed Kafka in the same region and VPC. Select a plan meeting the selected Kafka version's per-broker minimum. If creation reports “minimum 2 vCPU / 4 GiB RAM”, a three-broker cluster needs at least 6 vCPU / 12 GiB RAM in total; the total shown in the UI is not the per-broker size.
4. Create topic `fanout.events.v1` with at least 3 partitions, replication factor 3, and `min.insync.replicas=2`. Set retention beyond the longest supported fanout outage and disable unclean leader election. Give the application Kafka user read/write access to the topic and access to consumer group `fanout-v1`. This application intentionally does not create the topic during startup.
5. Download each cluster's own CA certificate. In Kafka **Overview → Connection Details**, select **VPC network → SASL** and the application user. Copy the entire displayed **hostname and port**. Public and private listeners can use different ports; adding `private-` to a public hostname while retaining its port is not sufficient. The `cloud` profile expects SASL/SCRAM-SHA-256 over TLS, not the mutual-TLS “SSL” connection option. See [Kafka connection details](https://docs.digitalocean.com/products/databases/kafka/how-to/connect/).

## 2. Connect the app to the VPC and permit database access

Choose the same VPC for the app and both managed clusters. If the database creation page does not offer a selector, inspect the cluster's **Overview → VPC Network** after creation and use that network for the app.

For an existing app, open **Apps → your app → Networking → Private Network → Edit network**, enable **Connect app to VPC network**, choose the network, and save. This redeploys the app. For a spec-based deployment, add a root-level `vpc` block with the selected network UUID; the supplied template does not contain an account-specific VPC. See [enable App Platform VPC](https://docs.digitalocean.com/products/app-platform/how-to/enable-vpc/).

```yaml
vpc:
  id: REPLACE_VPC_UUID
```

After the app receives its private egress IP, add it to **each cluster → Network Access → Add Trusted Sources**. PostgreSQL also supports adding the app as a trusted source; Kafka requires the app's VPC egress IP rather than selecting the App resource. Keep trusted sources enabled. Static **ingress** IPs shown under the app's Public Network are not its outbound database source addresses. See [App Platform database integration and Kafka trusted sources](https://docs.digitalocean.com/products/app-platform/how-to/manage-databases/).

If the app's private IP is still being assigned, you can temporarily permit the selected VPC's CIDR while completing initial setup, then narrow it to the app's private IP. A VPC CIDR permits every resource in that network and applies to private connections. See [Kafka VPC trusted-source guidance](https://docs.digitalocean.com/products/databases/kafka/concepts/best-practices/). Recheck these rules if you recreate the app or change its network.

## 3. Configure runtime variables

In the web service's **Environment variables** settings, add the following with runtime scope. Mark credentials as encrypted secrets. Remove any empty variable rows; every row needs a key. These are application runtime variables, separate from the [Postman client variables](../../README.md#test-with-postman).

| Variable | Value and where to obtain it |
|---|---|
| `SPRING_PROFILES_ACTIVE` | `cloud` |
| `FANOUT_API_KEY` | Generate with `openssl rand -hex 32`; use the same value as Postman's `apiKey`. This is the service's administrator key, not a DigitalOcean token. |
| `DB_URL` | JDBC URL using PostgreSQL **VPC network** host, port and selected database; format below. |
| `DB_USERNAME` | PostgreSQL user selected in Connection Details. |
| `DB_PASSWORD` | That PostgreSQL user's password; encrypted secret. |
| `POSTGRES_CA_CERT` | Entire contents of the PostgreSQL CA certificate, including PEM begin/end lines. |
| `KAFKA_BOOTSTRAP_SERVERS` | Complete `hostname:port` copied from Kafka **VPC network → SASL**; no `https://` prefix. |
| `KAFKA_SASL_JAAS_CONFIG` | One-line JAAS value below using the selected Kafka user/password; encrypted secret. |
| `KAFKA_CA_CERT` | Entire contents of the Kafka cluster's CA certificate. |
| `FANOUT_DELIVERY_ALLOW_HTTP` | `false` |
| `FANOUT_DELIVERY_ALLOW_PRIVATE_ADDRESSES` | `false` |

JDBC format, replacing every placeholder with the selected cluster's values:

```text
jdbc:postgresql://PG_VPC_HOST:PG_PORT/DATABASE_NAME?sslmode=verify-full&sslrootcert=/tmp/postgres-ca.crt
```

If the selected database is `defaultdb`, use `/defaultdb`; use `/fanout` only if that database exists. The container entrypoint writes `POSTGRES_CA_CERT` to `/tmp/postgres-ca.crt` with restrictive permissions before Java starts.

JAAS format:

```text
org.apache.kafka.common.security.scram.ScramLoginModule required username="YOUR_KAFKA_USER" password="YOUR_KAFKA_PASSWORD";
```

Keep the spaces, the double quotes around username/password, and the final semicolon. Do not add enclosing quotes around the whole value in the control-panel text field. Credentials containing quotes or backslashes need JAAS escaping. The `cloud` profile already sets `SASL_SSL`, `SCRAM-SHA-256`, a PEM trust store, and hostname verification.

Paste certificates as actual multiline PEM text, not a local filename or literal `\n` sequences. In YAML, use a block scalar (`value: |`) with indented certificate lines. Do not swap the PostgreSQL and Kafka certificates. Keep credentials and completed private specs out of Git and logs.

## 4. Build and deploy the web service

1. Push the project, including the current `Dockerfile`, to the GitHub branch connected to DigitalOcean. Local edits alone do not change App Platform's source.
2. In **App Platform → Create App**, choose the repository and branch, a **Web Service**, and the repository's Dockerfile. Keep the source directory at the repository root. For an existing app, edit its service settings instead of creating a duplicate.
3. Set HTTP port **8080**, route `/`, and readiness path **`/actuator/health/readiness`**. Leave Run Command empty so the Dockerfile entrypoint runs. App Platform terminates public HTTPS and forwards to the container port. See [manage App Platform services](https://docs.digitalocean.com/products/app-platform/how-to/manage-services/).
4. Configure the region, VPC, network access, and runtime variables described above. Disable **Auto-deploy on push** if using the verification-gated GitHub deployment workflow below.
5. Choose capacity, create/deploy, and follow **Build logs** and **Deploy/Runtime logs**. When live, copy the HTTPS app URL from **Live App** or **Networking → Domains** into Postman's `baseUrl`, without a trailing slash. The API has no homepage; `/` returning `404` does not indicate failure.

For a modest demonstration, one app instance with 1 GiB RAM is a starting point; watch its actual memory and database usage. Set:

```text
SPRING_DATASOURCE_HIKARI_MAXIMUMPOOLSIZE=8
SPRING_DATASOURCE_HIKARI_MINIMUMIDLE=2
```

The supplied spec uses **two instances**, and the application's default pool allows **16 connections per instance**. Two pools can therefore request 32 connections before accounting for rolling deployments or administration. A 1 GiB managed PostgreSQL plan currently allows 22 backend connections, so either reduce pools/instances or use a larger database. Budget headroom for overlapping deployments and migrations. See [PostgreSQL connection limits](https://docs.digitalocean.com/products/databases/postgresql/details/limits/).

Alternatively, copy [app.yaml](app.yaml) to a private, untracked location, replace the placeholders and capacity/network settings, then submit it with `doctl`:

```sh
doctl auth init
doctl apps spec validate /path/to/private-app.yaml
doctl apps create --spec /path/to/private-app.yaml --wait
```

Use the create command only for a new app; manage an existing app through its settings or `doctl apps update`. The [App Spec reference](https://docs.digitalocean.com/products/app-platform/reference/app-spec/) defines supported fields. Flyway coordinates migrations with a database lock; use backward-compatible migrations and backups for future rolling releases.

## 5. Verify delivery and collect evidence

```sh
curl --fail https://YOUR-APP-DOMAIN/actuator/health/readiness
curl --fail https://YOUR-APP-DOMAIN/api/v1/subscriptions \
  -H "X-API-Key: $FANOUT_API_KEY"
```

Readiness checks the application and database, not Kafka or successful webhook delivery. Complete the [DigitalOcean testing guide](../../docs/DigitalOcean-testing-guide.md): import both Postman files, register a real HTTPS receiver, ingest a matching event, and verify both the received webhook and its `DELIVERED` audit/attempt records. Then exercise filtering, idempotency, fanout, retry/recovery, exhaustion, ordering, replay, restart persistence, and deletion. Use a controlled receiver to verify signature validation and delivery-ID deduplication as well.

Save a green GitHub Actions push run and cloud test evidence for the assignment. Before accepting production traffic, also validate monitoring, alerts, managed-database backups, and a restore drill.

## Troubleshooting

| Symptom | Check or fix |
|---|---|
| `envs.key in body is required` during app creation | Remove rows with an empty key in both app-level and component-level environment variables. |
| `/app/entrypoint.sh: permission denied` before Java starts | Publish the current Dockerfile and rebuild. It sets permissions explicitly and invokes the script via `/bin/sh`. For an older image, set Run Command to `/bin/sh /app/entrypoint.sh`, then clear the override after deploying the fixed image. |
| Generic “Container Terminated” banner | Read detailed deployment logs; this banner alone does not prove an out-of-memory failure. |
| PostgreSQL `Connect timed out` | Check the whole JDBC host/port, app/DB VPC, private IP assignment, and PostgreSQL trusted sources. A TCP timeout precedes password/schema checks. |
| Database does not exist / permission denied for schema | Match the actual selected database/user; create the intended database and grant Flyway the required schema permissions. |
| Kafka bootstrap connects but broker nodes time out | Recopy **both host and port** from **VPC network → SASL**. Kafka discovers brokers after bootstrap; the wrong listener can return unreachable public broker addresses. Check Kafka trusted sources and routes to all advertised brokers. |
| Kafka TLS or authentication failure | Use Kafka's own CA and SASL credentials, preserve JAAS quotes/semicolon, and confirm the `cloud` profile. Never disable certificate or hostname verification to bypass the failure. |
| Health is `UP`, but `fanoutCompleted` stays false | Check topic existence, Kafka ACLs, broker connectivity, consumer lag, and the outbox. The database health check does not validate Kafka. |
| API `401` | Use `X-API-Key: {{apiKey}}` with the selected Postman environment and the exact runtime `FANOUT_API_KEY`. |
| API `415` / `Content-Type 'null'` | For POST, select **Body → raw → JSON** and send `Content-Type: application/json`. |

For Kafka network diagnosis, temporarily set `LOGGING_LEVEL_ORG_APACHE_KAFKA_CLIENTS=DEBUG` and inspect the host/port/IP used for each node. Remove this variable after diagnosis. A local JAAS “Successfully logged in” message alone does not prove every broker is reachable. `ssl.truststore.location = null` is normal when the cloud profile supplies inline PEM certificates.

## GitHub Actions: CI and deployment

CI runs **on GitHub**, while App Platform builds and hosts the deployed service. [Build and test](../../.github/workflows/ci.yml) runs on every push and pull request, executes `sh ./mvnw -B -ntp verify` using temporary PostgreSQL/Kafka Testcontainers, uploads reports, and builds the Docker image. Both workflows invoke the wrapper through `sh` so an executable bit lost during upload/export does not prevent Maven from starting. CI does not require the managed clusters or DigitalOcean credentials.

To enable the separate [manual deployment workflow](../../.github/workflows/deploy.yml):

1. In GitHub, create an environment named `production`, configure any required reviewers, and add its `DIGITALOCEAN_ACCESS_TOKEN` secret with app deployment permissions.
2. Point the App Platform app at this repository's `main` branch and disable automatic deployment, as in the supplied spec. Otherwise, a source push could deploy before GitHub tests pass.
3. Open **GitHub → Actions → Deploy to DigitalOcean → Run workflow**, select `main`, and enter the existing app ID from DigitalOcean's app URL or `doctl apps list`. The ID is not the app's public URL. The workflow reruns verification and requests a rebuild/deployment, waiting for completion.

This workflow deploys the current remote `main` head, not an immutable image from the earlier test job. Avoid concurrent pushes during deployment. For immutable release promotion, publish a digest-pinned image to a registry and deploy that tested digest. The workflow does not provision clusters or configure runtime credentials; complete those in DigitalOcean first.

## Recovery and scaling

- Kafka outages accumulate durable outbox work; restore connectivity and monitor catch-up. Do not delete unpublished outbox rows.
- Database loss requires managed backup/PITR recovery. After restore, consumers may have offsets ahead of restored fanout state. The outbox reconciler republishes retained events still awaiting fanout after five minutes, but cannot recreate events absent from the restored database. Validate restored state and duplicates before resuming traffic.
- Increase app instances for independent source streams after checking database capacity and subscriber rate limits. Database locks coordinate publication, row leases coordinate delivery, and a Kafka consumer group coordinates fanout. The relay remains deliberately serialized; measure it before large-volume workloads.
- Keep source partition count stable when relying on Kafka source ordering. The database ordering gate protects dispatch, but partition changes and replay deserve a controlled rollout and load test.
- Add egress controls, ingress rate limiting, tenant authorization, and retention policies before exposing this administrative API to untrusted clients.
