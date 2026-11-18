# DigitalOcean Deployment Testing Guide

Test the complete path: **API → PostgreSQL → Kafka → matching → webhook → delivery audit**. A successful deployment or health check alone does not verify notification delivery.

Use Postman against your live DigitalOcean HTTPS URL. Follow this sequence. For provisioning, runtime variables, or connection failures, use the [deployment guide](../deploy/digitalocean/README.md).

## 1. Set up Postman variables

In Postman, select **Import** and import both files from the repository:

- [Notification-Fanout-API.postman_collection.json](../postman/Notification-Fanout-API.postman_collection.json)
- [notification-fanout.postman_environment.json.json](../postman/notification-fanout.postman_environment.json.json)

Select **digital-ocean-env** as the active environment, then fill in its variables. The shared export intentionally leaves credentials, the receiver URL, and generated IDs empty.

| Variable | Value |
|---|---|
| `baseUrl` | Your DigitalOcean app's **Live App** HTTPS URL, without a trailing `/`; confirm the exported URL still points to your deployment |
| `apiKey` | The exact `FANOUT_API_KEY` set in the running app's runtime environment, not a DigitalOcean access token |
| `signingSecret` | A separate generated secret of at least 32 characters, for example the output of `openssl rand -hex 32` |
| `webhookUrl` | Set in step 3 |
| `subscriptionId` | Leave blank until step 4; copy the subscription response's `id` |
| `eventId` | Leave blank until step 5; copy the event response's `id` |
| `deliveryId` | Leave blank until step 6; copy the matching delivery object's `id` |

These IDs are returned by the API, not by DigitalOcean's console. The collection has no assertions, polling scripts, or automatic variable capture: save IDs manually and repeat GET requests as described below. It contains the smoke-test and replay requests; add or adapt requests for the additional scenarios in this guide. Follow the methods and URLs below, since some collection entries have the same name.

Alternatively, import the project's [OpenAPI specification](openapi.yaml) to generate requests for the full API. See the [Postman import instructions](https://learning.postman.com/docs/getting-started/importing-and-exporting/importing-data).

For all `/api/v1/...` requests, add:

```text
X-API-Key: {{apiKey}}
```

For POST requests, select **Body → raw → JSON**, which sets `Content-Type: application/json`.

For GET requests, use **Body → none**; they do not need an `Idempotency-Key`. If a request fails with `401`, check the selected environment, API key value, and enabled header. If a POST fails with `415`, check its JSON body mode and `Content-Type`. See also [Postman setup and troubleshooting](../README.md#test-with-postman).

## 2. Check health and authentication

Send:

```http
GET {{baseUrl}}/actuator/health/readiness
```

Expected: **200 OK** with:

```json
{"status":"UP"}
```

Then send:

```http
GET {{baseUrl}}/api/v1/subscriptions
```

Expected results:

| Request | Expected |
|---|---|
| Correct `X-API-Key` | `200`, JSON array |
| Missing or incorrect key | `401 Unauthorized` |

Readiness checks the application and database; the next steps verify Kafka and delivery.

## 3. Create a webhook receiver

Open [Webhook.site](https://webhook.site/) and copy **Your unique URL** into `webhookUrl`. Copy the receiving URL, not the browser’s dashboard address.

Keep that page open to inspect incoming notifications. Webhook.site provides a unique URL and displays the requests it receives. Use fictional event data for this test. See the [Webhook.site documentation](https://docs.webhook.site/).

## 4. Register a subscription

Send:

```http
POST {{baseUrl}}/api/v1/subscriptions
```

Body:

```json
{
  "webhookUrl": "{{webhookUrl}}",
  "signingSecret": "{{signingSecret}}",
  "ordered": true,
  "filter": {
    "eventTypes": ["order.created"],
    "sources": ["checkout"],
    "conditions": [
      {
        "path": "/total",
        "operator": "GTE",
        "value": 100
      }
    ]
  }
}
```

Expected: **201 Created**.

Copy the response’s `id` into `subscriptionId`. Confirm it appears in:

```http
GET {{baseUrl}}/api/v1/subscriptions
```

## 5. Submit a matching event

Send:

```http
POST {{baseUrl}}/api/v1/events
```

Add this header:

```text
Idempotency-Key: cloud-demo-001
```

Use a fresh key for a new test run. Reusing an existing key returns the original event, which may have been accepted before the subscription you just created.

Body:

```json
{
  "type": "order.created",
  "source": "checkout",
  "payload": {
    "orderId": "DO-001",
    "total": 150
  }
}
```

Expected: **202 Accepted**.

Save the response’s `id` as `eventId`. The `202` confirms durable acceptance; delivery happens asynchronously.

## 6. Verify receipt and audit history

Watch Webhook.site for the notification. Inspect its JSON body and these headers:

```text
X-Event-Id
X-Delivery-Id
X-Delivery-Attempt
X-Webhook-Timestamp
X-Webhook-Signature
```

Then send:

```http
GET {{baseUrl}}/api/v1/events/{{eventId}}
```

Eventually, `fanoutCompleted` should become `true`. This means matching finished.

Query the delivery:

```http
GET {{baseUrl}}/api/v1/deliveries?eventId={{eventId}}&subscriptionId={{subscriptionId}}
```

The result is an array and may initially be empty while fanout runs. Once the matching object appears, save **that object's** `id` as `deliveryId` and poll until **`status: "DELIVERED"`**. The subscription ID and event ID are different identifiers; do not use either in place of the delivery ID.

Query its attempts:

```http
GET {{baseUrl}}/api/v1/deliveries/{{deliveryId}}/attempts
```

Check `attemptNumber`, `startedAt`, `finishedAt`, `httpStatus`, and `outcome`. A successful receiver should produce a `2xx` HTTP status.

**This completes the first full cloud smoke test.**

## 7. Test filtering and idempotency

Use the event request above for these variations:

| Test | Change | Expected |
|---|---|---|
| Payload does not match | Set `total` to `50` | Accepted, but no delivery to this subscription |
| Type does not match | Set `type` to `order.cancelled` | No delivery to this subscription |
| Source does not match | Set `source` to `warehouse` | No delivery to this subscription |
| Identical retry | Original body and original `cloud-demo-001` key | Same event ID; no additional original delivery |
| Conflicting retry | Original key, but change the body | `409 Conflict` |
| Invalid request | Omit required `type` | `400 Bad Request` |

Give each **new event** a new idempotency key. Reuse a key only for the two idempotency tests. If repeating the entire guide, use a new key prefix for the new test run.

For nonmatching events, wait until `fanoutCompleted` is `true` before concluding that no delivery was created.

## 8. Test fanout to multiple subscribers

Create a second subscription with the same filter and a second webhook receiver URL. Then submit a **new matching event**.

Expected:

- Both receivers get the event.
- Each subscription has its own delivery record.
- Both notifications contain the same event ID but different delivery IDs.

The second subscription must exist **before** you submit this event. Keep both subscription IDs so you can query their deliveries separately.

## 9. Test retry, recovery, and terminal failure

Configure your test receiver to return **503** instead of 200. Webhook.site supports a configurable default response status; see its [response configuration](https://docs.webhook.site/api/tokens.html).

Submit a new matching event and inspect its attempts:

- Attempts record `httpStatus: 503`.
- The delivery remains `PENDING` while retries remain.
- `attemptCount` increases.
- Attempt timestamps show backoff; jitter means intervals are not exact.

Change the receiver back to **200** before retries are exhausted. The delivery should eventually become `DELIVERED`, preserving the earlier failed attempts.

For a separate failure test, leave the receiver returning 503. With the default policy, after **8 total attempts**, the delivery becomes `FAILED` and remains queryable.

## 10. Test per-source ordering

With the receiver healthy, send three matching events sequentially, waiting for each ingestion response:

```text
orderId: ORDER-1
orderId: ORDER-2
orderId: ORDER-3
```

Keep the same `source` and use different idempotency keys.

For the subscription created with `ordered: true`, verify delivery order using the receiver’s request history and `eventSequence`.

For a stronger check, make the receiver fail the first event temporarily. Later events for that same subscription/source should wait until the earlier delivery succeeds or becomes terminal.

## 11. Test replay and restart recovery

Restore the receiver to 200. Replay a range containing events accepted after this subscription was created:

```http
POST {{baseUrl}}/api/v1/subscriptions/{{subscriptionId}}/replays
```

Header:

```text
Idempotency-Key: cloud-replay-001
```

Example body covering 18 November 2026 in UTC:

```json
{
  "from": "2026-11-18T00:00:00Z",
  "to": "2026-11-19T00:00:00Z"
}
```

If testing on another date, adjust the range to include your events' `acceptedAt` timestamps. The range is inclusive of `from` and exclusive of `to`, and may span at most 31 days and select at most 10,000 eligible events. Update the bundled replay request's example key to `cloud-replay-001` (or a fresh replay key for this run). Reusing a replay key with a different range returns `409`; the same key and range returns the original replay.

Expected: **202 Accepted**, with `deliveryCount` greater than zero if eligible events exist.

Query:

```http
GET {{baseUrl}}/api/v1/deliveries?subscriptionId={{subscriptionId}}
```

Replay deliveries have new delivery IDs and a `replayId`. Original delivery history remains intact.

To verify persistence, restart the app from DigitalOcean and query your existing subscription, event, delivery, and attempts again. They should remain available. You can also restart while a test delivery is pending, then restore the receiver and verify retry processing resumes.

## 12. Test deletion last, then collect submission evidence

Send:

```http
DELETE {{baseUrl}}/api/v1/subscriptions/{{subscriptionId}}
```

Expected: **204 No Content**.

Submit another matching event with a new idempotency key. It should create no new delivery for the deleted subscription. Previously accepted work and audit history remain. Any other active matching subscription can still receive the event.

Finally, save evidence of:

- Successful webhook receipt and `DELIVERED` audit.
- Nonmatching event filtering.
- Multiple-subscriber fanout.
- Failed attempts followed by recovery, plus terminal failure.
- Ordering, replay, and persistence after restart.
- A green **GitHub Actions → Build and test** run on a push.

Remove the temporary Kafka `DEBUG` logging variable after troubleshooting:

```text
LOGGING_LEVEL_ORG_APACHE_KAFKA_CLIENTS
```

The automated suite in GitHub and these live cloud checks together provide useful assignment evidence.

Export the collection and a sanitized environment for the reviewer. Keep `baseUrl`, clear `apiKey`, `signingSecret`, `webhookUrl`, and generated IDs, and inspect exported files and saved response examples before sharing. The reviewer should fill in a private test credential and their own receiver URL, then generate fresh IDs by following this guide. Do not attach DigitalOcean tokens or database/Kafka passwords.
