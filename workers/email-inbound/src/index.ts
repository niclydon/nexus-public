/**
 * Nexus Email Inbound Worker
 *
 * Cloudflare Email Worker — catch-all for *@example.io
 *
 * Routes known agent addresses to the Nexus ingest API.
 * Forwards unknown addresses to a fallback (or drops them).
 *
 * Known addresses:
 *   aria@example.io → agent aria
 *   monitor@example.io → agent monitor
 *   modelops@example.io → agent model-ops
 *   collector@example.io → agent collector
 *   analyst@example.io → agent analyst
 *   fixer@example.io → agent fixer
 *   relationships@example.io → agent relationships
 *
 * Setup:
 *   1. Deploy: cd workers/email-inbound && npx wrangler deploy
 *   2. Set secret: npx wrangler secret put WEBHOOK_SECRET
 *   3. Cloudflare Dashboard → Email Routing: catch-all *@example.io → this worker
 */

export interface Env {
  WEBHOOK_URL: string;
  WEBHOOK_SECRET: string;
  FORWARD_TO: string; // fallback email for non-agent addresses
}

// Known agent local parts → agent IDs
const AGENT_MAP: Record<string, string> = {
  'aria': 'aria',
  'monitor': 'monitor',
  'modelops': 'model-ops',
  'collector': 'collector',
  'analyst': 'analyst',
  'fixer': 'fixer',
  'relationships': 'relationships',
};

function isAgentAddress(localPart: string): boolean {
  return localPart in AGENT_MAP;
}

function extractPlainText(raw: string): string {
  const boundaryMatch = raw.match(/boundary="?([^"\s;]+)"?/i);
  if (boundaryMatch) {
    const boundary = boundaryMatch[1];
    const parts = raw.split(`--${boundary}`);
    for (const part of parts) {
      if (part.includes('Content-Type: text/plain')) {
        const bodyStart = part.indexOf('\r\n\r\n') ?? part.indexOf('\n\n');
        if (bodyStart !== -1) {
          let text = part.slice(bodyStart + 4).trim().replace(/--$/, '').trim();
          if (part.includes('Content-Transfer-Encoding: quoted-printable')) {
            text = text.replace(/=\r?\n/g, '').replace(/=([0-9A-Fa-f]{2})/g, (_, hex) =>
              String.fromCharCode(parseInt(hex, 16)));
          }
          if (part.includes('Content-Transfer-Encoding: base64')) {
            try { text = atob(text.replace(/\s/g, '')); } catch { /* keep as-is */ }
          }
          return text;
        }
      }
    }
  }

  const headerEnd = raw.indexOf('\r\n\r\n') ?? raw.indexOf('\n\n');
  if (headerEnd !== -1) {
    const body = raw.slice(headerEnd + 4).trim();
    if (body.startsWith('<') || body.includes('<html')) {
      return body
        .replace(/<style[^>]*>[\s\S]*?<\/style>/gi, '')
        .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, '')
        .replace(/<br\s*\/?>/gi, '\n')
        .replace(/<\/p>/gi, '\n\n')
        .replace(/<[^>]*>/g, '')
        .replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
        .replace(/\n{3,}/g, '\n\n')
        .trim();
    }
    return body;
  }

  return raw.trim();
}

export default {
  async email(message: ForwardableEmailMessage, env: Env) {
    try {
    const from = message.from;
    const to = message.to.toLowerCase();
    const localPart = to.split('@')[0];
    const subject = message.headers.get('subject') || '(no subject)';

    console.log(`Inbound: from=${from} to=${to} subject="${subject}"`);

    // Not an agent address — forward to fallback or drop
    if (!isAgentAddress(localPart)) {
      console.log(`Not an agent address: ${to}`);
      if (env.FORWARD_TO) {
        console.log(`Forwarding to ${env.FORWARD_TO}`);
        await message.forward(env.FORWARD_TO);
      }
      return;
    }

    // Agent address — extract content and deliver to Nexus
    const rawBody = await new Response(message.raw).text();
    const body = extractPlainText(rawBody);
    const truncated = body.length > 10000 ? body.slice(0, 10000) + '\n\n[truncated]' : body;

    const payload = { from, to, subject, body: truncated };

    try {
      const response = await fetch(env.WEBHOOK_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Ingest-Token': env.WEBHOOK_SECRET,
        },
        body: JSON.stringify(payload),
      });

      if (response.ok) {
        const result = await response.json() as { agent?: string; inbox_id?: number };
        console.log(`Delivered to agent ${result.agent} (inbox ${result.inbox_id})`);
      } else {
        console.error(`Webhook failed: ${response.status} ${await response.text()}`);
        // Forward to fallback on webhook failure
        if (env.FORWARD_TO) {
          await message.forward(env.FORWARD_TO);
        }
      }
    } catch (err) {
      console.error(`Webhook error: ${err}`);
      if (env.FORWARD_TO) {
        await message.forward(env.FORWARD_TO);
      }
    }
    } catch (outerErr) {
      console.error(`Worker crash: ${outerErr}`);
    }
  },
};
