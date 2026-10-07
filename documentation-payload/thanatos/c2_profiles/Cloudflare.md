+++
title = "Cloudflare"
weight = 20
+++

# Cloudflare C2 Profile

Relays Mythic traffic through a Cloudflare Queues dead-drop channel behind a Cloudflare Worker front door. The implant never holds Cloudflare credentials: it only knows the Worker URL and a shared channel secret.

## Parameters

Parameter | Description
--------- | -----------
`worker_base_url` | Base URL of the deployed Cloudflare Worker (https)
`worker_secret` | Shared secret sent as the `X-Channel-Secret` header
`headers` | HTTP headers for callbacks, including `User-Agent`
`callback_interval` | Seconds between beacon cycles
`callback_jitter` | Jitter percentage applied to the callback interval
`encrypted_exchange_check` | Perform the RSA key exchange on first contact
`AESPSK` | Pre-shared key crypto (`aes256_hmac` or `none`)
`killdate` | Date the agent stops beaconing

## Behavior

Each beacon cycle sends the Mythic message to `POST {worker_base_url}/upload` as `{"id": <channel uuid>, "message": <base64 mythic blob>}` and then polls `GET {worker_base_url}/poll?id=<channel uuid>` for the reply, which normally arrives within one relay poll cycle (a few seconds). Messages larger than 100,000 characters are split into numbered parts that are reassembled on both sides, so file transfers up to 10 MB per message are supported.

The channel uuid is generated per process at startup, so running the same payload binary twice produces two separate callbacks with separate out-queues on the Cloudflare side.

If no reply arrives within the poll window (about 60 seconds), the beacon cycle fails and Thanatos exits following its normal connection-retry rules. Keep the Mythic cloudflare profile container running while callbacks are live.
