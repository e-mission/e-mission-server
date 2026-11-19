# Server image update GitHub App

This service listens for signed `registry_package.published` webhooks for
`ghcr.io/e-mission/e-mission-server`. It runs the Dependabot CLI Docker
updater for the internal server image in `op-admin-dashboard` and
`em-public-dashboard`. It uses the CLI's JSONL proposals to open one
App-authored PR per stable `vX.Y.Z` tag per dashboard. It does not enqueue a
GitHub-hosted Dependabot run, edit `dependabot.yml`, push to default branches,
or merge PRs. Existing hosted schedules remain unchanged.

The worker checks proposals against the exact dashboard commit it sent to
Dependabot. Only a bump to the published tag is accepted; production, dev,
and test pins in the public dashboard must all match before a PR is opened.
The CLI subprocess receives no GitHub credentials and only reads public
repositories and packages.

## Registration and deployment

Register a private GitHub App in `e-mission` and install it only on
`e-mission-server`, `op-admin-dashboard`, and `em-public-dashboard`. Subscribe
to **Registry package** events and grant **Packages: read**, **Contents: read
and write**, and **Pull requests: read and write**. The runtime scopes its
installation tokens to the two dashboards, without package permissions.
Keep branch protection enabled.

Run on a dedicated host with Node.js 22+, Docker, and a pinned, supported
Dependabot CLI binary. Keep CLI image verification enabled. Set `APP_ID`,
`INSTALLATION_ID`, `APP_PRIVATE_KEY_FILE` (the path to the secret-mounted PEM),
and `WEBHOOK_SECRET` from the deployment's secret store. Set `DEPENDABOT_CLI`
to the absolute path to the binary and `QUEUE_DIR` to an absolute path on a
persistent volume. Use one process per queue. Run
`node tools/dependabot-publish-app/app.mjs` behind an HTTPS proxy, configure
GitHub to send events to `/webhook`, and route traffic to `PORT` (default
`8080`). Do not commit credentials.

Webhook acknowledgement happens after both jobs are queued. Jobs are polled
at five-minute intervals, and failures stay queued for retry. Inspect service
logs for stale jobs (for example if the registry now only proposes a different
tag) before removing the corresponding queue file. A redelivery will not
reopen an existing PR.

## Verification

Run `node --test tools/dependabot-publish-app/app.test.mjs` from the repository
root. Tests use synthetic webhook deliveries, simulated Dependabot JSONL
records, and a mocked GitHub API. In staging, verify a real multi-platform
GHCR delivery contains a stable tag, then verify the CLI proposes the expected
files and the App opens the two PRs before enabling the production webhook.
