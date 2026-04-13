/**
 * GitHub API utility — read/write access for self-improvement system.
 */

const GITHUB_OWNER = 'owner';
const GITHUB_API = 'https://api.github.com';
const TIMEOUT_MS = 15_000;

export function isGithubConfigured(): boolean {
  return !!process.env.GITHUB_PAT;
}

async function githubFetch(path: string, options?: { method?: string; body?: unknown }): Promise<Response> {
  const pat = process.env.GITHUB_PAT;
  if (!pat) throw new Error('GITHUB_PAT not configured');

  const response = await fetch(`${GITHUB_API}${path}`, {
    method: options?.method ?? 'GET',
    signal: AbortSignal.timeout(TIMEOUT_MS),
    headers: {
      Authorization: `token ${pat}`,
      Accept: 'application/vnd.github.v3+json',
      'Content-Type': 'application/json',
      'User-Agent': 'ARIA-Tempo/1.0',
    },
    ...(options?.body ? { body: JSON.stringify(options.body) } : {}),
  });

  if (!response.ok) {
    const errorBody = await response.text().catch(() => '');
    throw new Error(`GitHub API ${options?.method ?? 'GET'} ${path} failed: ${response.status}${errorBody ? ` -- ${errorBody.slice(0, 500)}` : ''}`);
  }
  return response;
}

export async function githubReadFile(repo: string, path: string, branch?: string): Promise<{ content: string; sha: string }> {
  const query = branch ? `?ref=${encodeURIComponent(branch)}` : '';
  const response = await githubFetch(`/repos/${GITHUB_OWNER}/${repo}/contents/${path}${query}`);
  const data = (await response.json()) as { content?: string; sha?: string };
  if (!data.content || !data.sha) throw new Error(`Unexpected response for ${repo}/${path}`);
  return { content: Buffer.from(data.content, 'base64').toString('utf-8'), sha: data.sha };
}

export async function githubCreateBranch(repo: string, branchName: string): Promise<void> {
  const refResponse = await githubFetch(`/repos/${GITHUB_OWNER}/${repo}/git/ref/heads/main`);
  const refData = (await refResponse.json()) as { object?: { sha?: string } };
  const sha = refData.object?.sha;
  if (!sha) throw new Error(`Could not get HEAD SHA for ${repo}/main`);

  await githubFetch(`/repos/${GITHUB_OWNER}/${repo}/git/refs`, {
    method: 'POST',
    body: { ref: `refs/heads/${branchName}`, sha },
  });
}

export async function githubCreateOrUpdateFile(
  repo: string, path: string, content: string, message: string, branch: string, existingSha?: string,
): Promise<string> {
  const body: Record<string, unknown> = { message, content: Buffer.from(content).toString('base64'), branch };
  if (existingSha) body.sha = existingSha;

  const response = await githubFetch(`/repos/${GITHUB_OWNER}/${repo}/contents/${path}`, { method: 'PUT', body });
  const data = (await response.json()) as { commit?: { sha?: string } };
  return data.commit?.sha ?? '';
}

export async function githubCreatePR(
  repo: string, params: { title: string; body: string; head: string; base?: string },
): Promise<{ url: string; number: number }> {
  const response = await githubFetch(`/repos/${GITHUB_OWNER}/${repo}/pulls`, {
    method: 'POST',
    body: { title: params.title, body: params.body, head: params.head, base: params.base ?? 'main' },
  });
  const data = (await response.json()) as { html_url?: string; number?: number };
  return { url: data.html_url ?? '', number: data.number ?? 0 };
}
