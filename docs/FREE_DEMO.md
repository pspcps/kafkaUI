# Free interactive preview on Vercel

This branch provides a browser-only demo of the existing KafkaUI frontend. No Kafka broker, Go server, management database, Kubernetes cluster or cloud credentials are required. All displayed data is synthetic. Mutations are stored only in the current browser's localStorage; Reset demo restores the seed data. Do not enter real credentials or customer data.

## Deploy

Use a Vercel Hobby account for an eligible personal preview, within its free plan limits. Verify the current terms at https://vercel.com/docs/plans/hobby before deploying a commercial service.

1. Import `pspcps/kafkaUI` into Vercel.
2. Select the free demo branch, `codex/free-hosted-demo`.
3. Set the project's Root Directory to `frontend` and Framework Preset to Next.js.
4. Use the Hobby plan. Do not add paid storage, domains, integrations or other services.
5. Deploy. `frontend/vercel.json` runs `npm run build:demo`, embedding `NEXT_PUBLIC_DEMO_MODE=true` in the browser bundle.

No secrets or backend environment variables are needed. This configuration intentionally deploys a demo; do not merge its Vercel configuration into a production deployment without changing the build command and supplying a secured API.

## Explore

- Cluster dashboards and broker metadata for development and staging.
- Topic search, partitions and configuration.
- Browse/filter synthetic messages; produce a sample message.
- Create and delete demo topics.
- Topic Consumers tab with links to matching groups.
- Consumer groups with multiple topic summaries, closed-by-default partition details, known lag and links back to topics.
- Whole-message and individual key/value/header/metadata clipboard controls.
- Sample replication jobs with simulated pause/resume and scaling. New jobs select simulated managed connections instead of requesting passwords.
- The old version 1 demo state is removed from browser storage because it previously allowed inline password inputs.
- Schema editing stores local examples. Server-side schema validation and Kafka offset resets explicitly report that they require a live backend.

Actions are simulations and do not prove Kafka or worker integration. Synthetic metric values are fixed examples, not live measurements. The demo provides convenience entry without login; it grants no access to a real backend and sends no API requests. Resetting the demo clears only demo data.

## Local check

From `frontend`:

```sh
npm ci
npm run build:demo
npm start -- --port 3001
```

To run the real application, use `npm run build` without `NEXT_PUBLIC_DEMO_MODE`, configure its backend and follow SETUP.md. The real application continues to require server authentication and authorization.

## Validation

The demo production build and browser smoke checks pass: dashboard, topic search, message browsing/production, persistence after reload, sample jobs, topic creation and reset. The browser makes no backend API requests during these checks. `npm audit --omit=dev` reports zero known production dependency vulnerabilities at this revision; this is not a security certification. The demo branch upgrades Next.js to 16.4.6 and React to 19; the security PR remains separate. The obsolete `next lint` script is removed in this branch; configure ESLint separately before making lint a release gate.

With Playwright installed and the demo running, execute:

```sh
DEMO_BASE_URL=http://localhost:3001 python scripts/test_demo.py
```

Set `PLAYWRIGHT_CHROMIUM_EXECUTABLE` only when using a preinstalled Chromium instead of Playwright's managed browser.
