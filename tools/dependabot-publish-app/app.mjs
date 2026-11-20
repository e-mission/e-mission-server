import { createHmac, sign, timingSafeEqual } from 'node:crypto';
import { execFile } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { mkdir, mkdtemp, readdir, rmdir, unlink, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { isAbsolute, join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { promisify } from 'node:util';

const owner = 'e-mission';
const image = 'ghcr.io/e-mission/e-mission-server';
const manifests = {
  'op-admin-dashboard': ['docker/Dockerfile'],
  'em-public-dashboard': ['viz_scripts/Dockerfile', 'viz_scripts/Dockerfile.dev', 'viz_scripts/tests/Dockerfile.test'],
};
const dashboards = Object.keys(manifests);
const releaseTag = /^v(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)$/;
const run = promisify(execFile);

export function publishedTag(event, payload) {
  const pkg = payload?.registry_package;
  if (event !== 'registry_package' || payload?.action !== 'published' ||
      payload.repository?.full_name !== 'e-mission/e-mission-server' ||
      pkg?.name !== 'e-mission-server' || pkg.namespace !== owner ||
      pkg.package_type !== 'container') return;
  const version = pkg.package_version;
  return [version?.container_metadata?.tag?.name, version?.tag_name, version?.version].find(
    tag => typeof tag === 'string' && tag.length <= 128 && releaseTag.test(tag),
  );
}

export function createHandler(secret, update) {
  return async (headers, raw) => {
    const signature = headers['x-hub-signature-256'];
    const expected = createHmac('sha256', secret).update(raw).digest();
    if (typeof signature !== 'string' || !/^sha256=[a-f0-9]{64}$/.test(signature) ||
        !timingSafeEqual(expected, Buffer.from(signature.slice(7), 'hex'))) return 401;
    let payload;
    try { payload = JSON.parse(raw); } catch { return 400; }
    const tag = publishedTag(headers['x-github-event'], payload);
    if (!tag) return 204;
    const outcomes = await Promise.allSettled(dashboards.map(repo => update(repo, tag)));
    if (outcomes.some(outcome => outcome.status === 'rejected')) {
      throw new AggregateError(outcomes.flatMap(outcome =>
        outcome.status === 'rejected' ? [outcome.reason] : []), `Unable to process ${tag} for both dashboards`);
    }
    return 202;
  };
}

export function cliJob(repo, head) {
  const dirs = [...new Set(manifests[repo].map(path => `/${path.slice(0, path.lastIndexOf('/'))}`))];
  return { job: {
    'package-manager': 'docker',
    'allowed-updates': [{ 'dependency-name': image }],
    ...(dirs.length > 1 && { 'dependency-groups': [{
      name: 'notebook-server-base', rules: { patterns: [image] },
    }] }),
    source: { provider: 'github', repo: `${owner}/${repo}`, commit: head,
      ...(dirs.length === 1 ? { directory: dirs[0] } : { directories: dirs }) },
  } };
}

export async function runCli(job, binary) {
  const env = Object.fromEntries(['PATH', 'HOME', 'DOCKER_HOST', 'DOCKER_CONFIG', 'XDG_RUNTIME_DIR']
    .filter(key => process.env[key]).map(key => [key, process.env[key]]));
  const dir = await mkdtemp(join(tmpdir(), 'server-image-update-'));
  const input = join(dir, 'job.json');
  try {
    await writeFile(input, JSON.stringify(job), { mode: 0o600 });
    const { stdout } = await run(binary, ['update', '--file', input, '--timeout', '15m'], {
      env, timeout: 16 * 60_000, maxBuffer: 8 * 1024 * 1024,
    });
    return stdout.split('\n').filter(line => line.startsWith('{')).map(line => JSON.parse(line));
  } finally {
    try { await unlink(input); }
    catch (error) { if (error.code !== 'ENOENT') throw error; }
    await rmdir(dir);
  }
}

function pinnedTag(contents) {
  const tags = [...contents.matchAll(/^FROM[ \t]+ghcr\.io\/e-mission\/e-mission-server:([^\s]+)[ \t]*\r?$/gm)];
  if (tags.length !== 1 || !releaseTag.test(tags[0][1])) throw new Error('Expected exactly one stable server image pin');
  return tags[0][1];
}

function compareTags(a, b) {
  const parts = b.slice(1).split('.').map(BigInt);
  for (const [i, part] of a.slice(1).split('.').map(BigInt).entries()) {
    if (part !== parts[i]) return part > parts[i] ? 1 : -1;
  }
  return 0;
}

export async function proposeUpdate(api, check, repo, tag) {
  if (!dashboards.includes(repo) || !releaseTag.test(tag)) throw new Error('Unsupported repository or tag');
  const prefix = `/repos/${owner}/${repo}`;
  const branch = `server-image-update/${tag}`;
  const pulls = `${prefix}/pulls?state=all&head=${encodeURIComponent(`${owner}:${branch}`)}`;
  if ((await api('GET', pulls)).length) return;
  const { default_branch: base } = await api('GET', prefix);
  const { object: { sha: head } } = await api('GET', `${prefix}/git/ref/heads/${base}`);
  const read = async (path, ref) => Buffer.from((await api('GET',
    `${prefix}/contents/${path}?ref=${encodeURIComponent(ref)}`)).content, 'base64').toString('utf8');
  const expected = new Map();
  for (const path of manifests[repo]) {
    const original = await read(path, head);
    const pin = pinnedTag(original);
    if (compareTags(pin, tag) > 0) return; // A later release is already in use; never downgrade.
    if (pin !== tag) {
      expected.set(path, original.replace(
        /^(FROM[ \t]+ghcr\.io\/e-mission\/e-mission-server:)[^\s]+/m, `$1${tag}`,
      ));
    }
  }
  if (!expected.size) return;

  const events = await check(cliJob(repo, head));
  const proposed = new Map();
  for (const { type, data } of events) {
    if (type === 'record_update_job_error' || type === 'record_update_job_unknown_error') {
      throw new Error(`Dependabot reported an update error for ${repo}`);
    }
    if (type !== 'create_pull_request') continue;
    if (data['base-commit-sha'] !== head) throw new Error('Dependabot result used a different commit');
    for (const file of data['updated-dependency-files']) {
      const path = `${file.directory}/${file.name}`.replace(/^\/+/, '');
      if (file.deleted || !expected.has(path) || expected.get(path) !== file.content || proposed.has(path)) {
        throw new Error(`Unexpected Dependabot update for ${repo}: ${path}`);
      }
      proposed.set(path, file.content);
    }
  }
  if (proposed.size !== expected.size) throw new Error(`No complete Dependabot update to ${tag} for ${repo}`);

  const { tree } = await api('GET', `${prefix}/git/commits/${head}`);
  const change = await api('POST', `${prefix}/git/trees`, {
    base_tree: tree.sha,
    tree: [...proposed].map(([path, content]) => ({ path, mode: '100644', type: 'blob', content })),
  });
  const title = `Update the server image to ${tag}`;
  const commit = await api('POST', `${prefix}/git/commits`, {
    message: title, tree: change.sha, parents: [head],
  });
  try {
    await api('POST', `${prefix}/git/refs`, { ref: `refs/heads/${branch}`, sha: commit.sha });
  } catch (error) {
    if (![409, 422].includes(error.status)) throw error;
    if ((await api('GET', pulls)).length) return;
    for (const [path, content] of proposed) {
      if (await read(path, branch) !== content) throw error;
    }
  }
  try {
    await api('POST', `${prefix}/pulls`, {
      title, head: branch, base,
      body: `The Dependabot CLI proposed this update after \`${image}:${tag}\` was published. ` +
        'This GitHub App opened the PR; it is not a GitHub-hosted Dependabot run.',
    });
  } catch (error) {
    if (error.status !== 422 || !(await api('GET', pulls)).length) throw error;
  }
}

async function github(method, path, token, body) {
  const response = await fetch(`https://api.github.com${path}`, {
    method, redirect: 'error', signal: AbortSignal.timeout(8_000),
    headers: {
      Authorization: `Bearer ${token}`, Accept: 'application/vnd.github+json',
      'Content-Type': 'application/json', 'X-GitHub-Api-Version': '2022-11-28',
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!response.ok) {
    throw Object.assign(new Error(`GitHub ${method} ${path}: HTTP ${response.status}`), { status: response.status });
  }
  return response.json();
}

function installTokenProvider(appId, privateKey, installation) {
  let token;
  return () => {
    if (token) return token;
    const now = Math.floor(Date.now() / 1000);
    const encode = value => Buffer.from(JSON.stringify(value)).toString('base64url');
    const claims = `${encode({ alg: 'RS256', typ: 'JWT' })}.${encode({ iss: appId, iat: now - 60, exp: now + 540 })}`;
    const jwt = `${claims}.${sign('RSA-SHA256', Buffer.from(claims), privateKey).toString('base64url')}`;
    token = github('POST', `/app/installations/${installation}/access_tokens`, jwt, {
      repositories: dashboards, permissions: { contents: 'write', pull_requests: 'write' },
    }).then(result => result.token);
    return token;
  };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const { APP_ID, INSTALLATION_ID, APP_PRIVATE_KEY_FILE, WEBHOOK_SECRET, QUEUE_DIR,
    DEPENDABOT_CLI, PORT = '8080' } = process.env;
  if (!APP_ID || !/^\d+$/.test(INSTALLATION_ID ?? '') || !APP_PRIVATE_KEY_FILE || !WEBHOOK_SECRET ||
      !QUEUE_DIR || !isAbsolute(QUEUE_DIR) || !DEPENDABOT_CLI || !isAbsolute(DEPENDABOT_CLI)) {
    throw new Error('Set App credentials, QUEUE_DIR and DEPENDABOT_CLI (absolute paths)');
  }
  const key = readFileSync(APP_PRIVATE_KEY_FILE);
  await mkdir(QUEUE_DIR, { recursive: true, mode: 0o700 });
  const enqueue = async (repo, tag) => {
    try { await writeFile(join(QUEUE_DIR, `${repo}_${tag}`), '', { flag: 'wx', mode: 0o600 }); }
    catch (error) { if (error.code !== 'EEXIST') throw error; }
  };
  // A persistent volume and one worker per queue avoid losing slow jobs on restart.
  async function drain() {
    for (const entry of await readdir(QUEUE_DIR)) {
      const [repo, tag, extra] = entry.split('_');
      if (extra || !dashboards.includes(repo) || !releaseTag.test(tag)) continue;
      try {
        const token = installTokenProvider(APP_ID, key, INSTALLATION_ID);
        await proposeUpdate((method, path, body) => token().then(value => github(method, path, value, body)),
          job => runCli(job, DEPENDABOT_CLI), repo, tag);
        await unlink(join(QUEUE_DIR, entry));
      } catch (error) {
        console.error(`Dependency check failed for ${repo} ${tag}: ${error.code ?? error.message}`);
      }
    }
  }
  async function poll() {
    try { await drain(); }
    catch (error) { console.error(`Unable to read update queue: ${error.message}`); }
    setTimeout(poll, 300_000);
  }
  void poll();
  const handle = createHandler(WEBHOOK_SECRET, enqueue);
  createServer(async (req, res) => {
    if (req.method !== 'POST' || req.url !== '/webhook') return res.writeHead(404).end();
    try {
      const chunks = [];
      let size = 0;
      for await (const chunk of req.iterator({ destroyOnReturn: false })) {
        size += chunk.length;
        if (size > 1024 * 1024) {
          res.writeHead(413, { Connection: 'close' }).end();
          return;
        }
        chunks.push(chunk);
      }
      res.writeHead(await handle(req.headers, Buffer.concat(chunks))).end();
    } catch (error) {
      console.error(error);
      res.writeHead(502).end();
    }
  }).listen(Number(PORT));
}
