import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { test } from 'node:test';
import { createHandler, proposeUpdate } from './app.mjs';

const secret = 'fixture-secret';
const payload = {
  action: 'published',
  repository: { full_name: 'e-mission/e-mission-server' },
  registry_package: {
    name: 'e-mission-server', namespace: 'e-mission', package_type: 'container',
    package_version: { container_metadata: { tag: { name: 'v4.1.0' } } },
  },
};

function delivery(handler, data = payload, event = 'registry_package', valid = true) {
  const raw = Buffer.from(JSON.stringify(data));
  const signature = createHmac('sha256', secret).update(valid ? raw : 'tampered').digest('hex');
  return handler({ 'x-github-event': event, 'x-hub-signature-256': `sha256=${signature}` }, raw);
}

test('rejects tampered payloads and ignores unrelated events', async () => {
  const handle = createHandler(secret, () => assert.fail('Unexpected update'));
  assert.equal(await delivery(handle, payload, 'registry_package', false), 401);
  assert.equal(await delivery(handle, payload, 'release'), 204);
  assert.equal(await delivery(handle, { ...payload, action: 'updated' }), 204);
  assert.equal(await delivery(handle, { ...payload, repository: { full_name: 'other/e-mission-server' } }), 204);
  const byDigest = structuredClone(payload);
  byDigest.registry_package.package_version.container_metadata.tag.name = 'sha256:123';
  assert.equal(await delivery(handle, byDigest), 204);
});

function fakeGitHub({ failOnce = false } = {}) {
  const image = 'ghcr.io/e-mission/e-mission-server';
  const before = `FROM ${image}:v4.0.1\nRUN echo unchanged\n`;
  const after = before.replace('v4.0.1', 'v4.1.0');
  const state = { refs: new Map(), prs: new Map(), writes: [] };
  const admin = ['docker/Dockerfile'];
  const publicDash = ['viz_scripts/Dockerfile', 'viz_scripts/Dockerfile.dev', 'viz_scripts/tests/Dockerfile.test'];
  const commits = new Map();
  const job = async ({ job }) => {
    assert.equal(job['package-manager'], 'docker');
    assert.deepEqual(job['allowed-updates'], [{ 'dependency-name': image }]);
    const isAdmin = job.source.repo === 'e-mission/op-admin-dashboard';
    const paths = isAdmin ? admin : publicDash;
    if (isAdmin) assert.equal(job.source.directory, '/docker');
    else {
      assert.deepEqual(job.source.directories, ['/viz_scripts', '/viz_scripts/tests']);
      assert.deepEqual(job['dependency-groups'][0].rules.patterns, [image]);
    }
    return [{ type: 'create_pull_request', data: {
      'base-commit-sha': job.source.commit,
      'updated-dependency-files': paths.map(path => ({
        directory: `/${path.slice(0, path.lastIndexOf('/'))}`, name: path.split('/').at(-1), content: after,
      })),
    } }];
  };
  const api = async (method, path, body) => {
    const [, repo, route] = /^\/repos\/e-mission\/([^/]+)(.*)$/.exec(path);
    if (method === 'GET') {
      if (route.startsWith('/pulls?')) return state.prs.has(repo) ? [state.prs.get(repo)] : [];
      if (route === '') return { default_branch: repo === 'op-admin-dashboard' ? 'master' : 'main' };
      if (route.startsWith('/git/ref/heads/')) return { object: { sha: 'base-sha' } };
      if (route.startsWith('/git/commits/')) return { tree: { sha: 'base-tree' } };
      if (route.includes('/contents/')) {
        const path = route.split('?')[0].slice('/contents/'.length);
        const content = route.endsWith('base-sha') ? before : state.refs.get(repo).files.get(path);
        return { content: Buffer.from(content).toString('base64') };
      }
    }
    state.writes.push({ repo, route, body });
    if (route === '/git/trees') {
      const sha = `${repo}-proposal`;
      commits.set(sha, new Map(body.tree.map(({ path, content }) => [path, content])));
      return { sha };
    }
    if (route === '/git/commits') return { sha: body.tree };
    if (route === '/git/refs') {
      if (state.refs.has(repo)) throw Object.assign(new Error('Ref exists'), { status: 422 });
      state.refs.set(repo, { ref: body.ref, files: commits.get(body.sha) });
      return {};
    }
    if (route === '/pulls') {
      if (failOnce && repo === 'em-public-dashboard') {
        failOnce = false;
        throw Object.assign(new Error('Temporary failure'), { status: 503 });
      }
      if (state.prs.has(repo)) throw Object.assign(new Error('PR exists'), { status: 422 });
      state.prs.set(repo, body);
      return {};
    }
    assert.fail(`${method} ${path}`);
  };
  return { api, state, job, after };
}

test('creates one CLI-backed PR per dashboard, only on new branches', async () => {
  const { api, state, job, after } = fakeGitHub();
  const handle = createHandler(secret, (repo, tag) => proposeUpdate(api, job, repo, tag));
  assert.deepEqual(await Promise.all([delivery(handle), delivery(handle)]), [202, 202]);
  const previous = state.writes.length;
  assert.equal(await delivery(handle), 202);
  assert.equal(state.writes.length, previous);
  assert.equal(state.prs.size, 2);
  for (const [repo, base] of [['op-admin-dashboard', 'master'], ['em-public-dashboard', 'main']]) {
    assert.equal(state.prs.get(repo).base, base);
    assert.equal(state.prs.get(repo).head, 'server-image-update/v4.1.0');
    assert.equal(state.refs.get(repo).ref, 'refs/heads/server-image-update/v4.1.0');
    const files = state.refs.get(repo).files;
    assert.equal(files.size, base === 'master' ? 1 : 3);
    for (const content of files.values()) assert.equal(content, after);
  }
});

test('a redelivery recovers an interrupted PR without recreating the successful PR', async () => {
  const { api, state, job } = fakeGitHub({ failOnce: true });
  const handle = createHandler(secret, (repo, tag) => proposeUpdate(api, job, repo, tag));
  await assert.rejects(delivery(handle), AggregateError);
  assert.equal(state.prs.size, 1);
  assert.equal(await delivery(handle), 202);
  assert.equal(state.prs.size, 2);
  assert.equal(state.writes.filter(write => write.repo === 'op-admin-dashboard' && write.route === '/pulls').length, 1);
});

test('rejects unrelated edits, incomplete groups, and stale releases before writing', async () => {
  const { api, state, job } = fakeGitHub();
  const draft = override => async input => {
    const events = await job(input);
    override(events[0].data['updated-dependency-files']);
    return events;
  };
  await assert.rejects(proposeUpdate(api, draft(files => { files[0].content += 'RUN unexpected\n'; }),
    'op-admin-dashboard', 'v4.1.0'), /Unexpected Dependabot update/);
  await assert.rejects(proposeUpdate(api, draft(files => files.pop()),
    'em-public-dashboard', 'v4.1.0'), /No complete Dependabot update/);
  await proposeUpdate(api, () => assert.fail('CLI must not downgrade'), 'op-admin-dashboard', 'v4.0.0');
  assert.equal(state.writes.length, 0);
});
