/**
 * mbox parser — Parses .mbox files (Gmail Takeout format) into individual emails.
 * Filters out Spam, Trash, Drafts, and promotional/social category emails.
 */

const SKIP_LABELS = new Set([
  'spam', 'trash', 'drafts', 'draft',
  'category promotions', 'category social', 'category updates', 'category forums',
]);

export interface ParsedEmail {
  from: string;
  to: string;
  cc: string;
  subject: string;
  date: string;
  messageId: string;
  labels: string[];
  body: string;
  threadId: string;
  index: number;
}

export interface MboxParseResult {
  emails: ParsedEmail[];
  totalFound: number;
  skippedSpam: number;
  skippedTrash: number;
  skippedDrafts: number;
  skippedPromotions: number;
  skippedOther: number;
}

export function parseMbox(content: string): MboxParseResult {
  const result: MboxParseResult = {
    emails: [], totalFound: 0,
    skippedSpam: 0, skippedTrash: 0, skippedDrafts: 0, skippedPromotions: 0, skippedOther: 0,
  };

  const normalized = content.replace(/\r\n/g, '\n');
  const rawMessages = splitMboxMessages(normalized);
  result.totalFound = rawMessages.length;

  for (let i = 0; i < rawMessages.length; i++) {
    const raw = rawMessages[i];
    if (!raw?.trim()) continue;
    const parsed = parseRawMessage(raw, i);
    if (!parsed) continue;

    const skipReason = shouldSkip(parsed.labels);
    if (skipReason) {
      switch (skipReason) {
        case 'spam': result.skippedSpam++; break;
        case 'trash': result.skippedTrash++; break;
        case 'drafts': case 'draft': result.skippedDrafts++; break;
        case 'category promotions': result.skippedPromotions++; break;
        default: result.skippedOther++; break;
      }
      continue;
    }
    result.emails.push(parsed);
  }
  return result;
}

function splitMboxMessages(content: string): string[] {
  const messages: string[] = [];
  const separator = /^From /gm;
  const boundaries: number[] = [];
  let match: RegExpExecArray | null;
  while ((match = separator.exec(content)) !== null) boundaries.push(match.index);
  if (boundaries.length === 0) return content.trim().length > 0 ? [content] : [];

  for (let i = 0; i < boundaries.length; i++) {
    const start = boundaries[i];
    const end = i + 1 < boundaries.length ? boundaries[i + 1] : content.length;
    const msg = content.slice(start, end);
    const firstNewline = msg.indexOf('\n');
    if (firstNewline > 0) messages.push(msg.slice(firstNewline + 1));
  }
  return messages;
}

function parseRawMessage(raw: string, index: number): ParsedEmail | null {
  const headerEndIdx = raw.indexOf('\n\n');
  if (headerEndIdx < 0) return null;

  const headers = parseHeaders(raw.slice(0, headerEndIdx));
  const bodySection = raw.slice(headerEndIdx + 2);

  const subject = decodeHeaderValue(headers['subject'] || '(no subject)');
  const labelsRaw = headers['x-gmail-labels'] || '';
  const labels = labelsRaw.split(',').map(l => l.trim()).filter(l => l.length > 0);
  const body = extractPlainTextBody(bodySection, headers['content-type'] || '', headers['content-transfer-encoding'] || '');
  if (!body || body.trim().length < 5) return null;

  return {
    from: headers['from'] || '', to: headers['to'] || '', cc: headers['cc'] || '',
    subject, date: headers['date'] || '', messageId: headers['message-id'] || '',
    labels, body, threadId: headers['x-gm-thrid'] || headers['message-id'] || '', index,
  };
}

function parseHeaders(headerSection: string): Record<string, string> {
  const headers: Record<string, string> = {};
  let currentKey = '', currentValue = '';
  for (const line of headerSection.split('\n')) {
    if (line.startsWith(' ') || line.startsWith('\t')) {
      currentValue += ' ' + line.trim();
    } else {
      if (currentKey) headers[currentKey.toLowerCase()] = currentValue;
      const colonIdx = line.indexOf(':');
      if (colonIdx > 0) { currentKey = line.slice(0, colonIdx).trim(); currentValue = line.slice(colonIdx + 1).trim(); }
    }
  }
  if (currentKey) headers[currentKey.toLowerCase()] = currentValue;
  return headers;
}

function decodeHeaderValue(value: string): string {
  return value.replace(/=\?([^?]+)\?([BQ])\?([^?]+)\?=/gi, (_match, _charset, encoding, encoded) => {
    try {
      if (encoding.toUpperCase() === 'B') return Buffer.from(encoded, 'base64').toString('utf-8');
      if (encoding.toUpperCase() === 'Q') return encoded.replace(/_/g, ' ').replace(/=([0-9A-Fa-f]{2})/g, (_: string, hex: string) => String.fromCharCode(parseInt(hex, 16)));
    } catch { /* fall through */ }
    return value;
  });
}

function extractPlainTextBody(body: string, contentType: string, transferEncoding: string): string {
  const boundaryMatch = contentType.match(/boundary="?([^";\s]+)"?/i);
  if (boundaryMatch) return extractFromMultipart(body, boundaryMatch[1]);

  let decoded = body;
  if (transferEncoding.toLowerCase().includes('base64')) {
    try { decoded = Buffer.from(body.replace(/\s/g, ''), 'base64').toString('utf-8'); } catch { decoded = body; }
  } else if (transferEncoding.toLowerCase().includes('quoted-printable')) {
    decoded = decodeQuotedPrintable(body);
  }
  if (contentType.toLowerCase().includes('text/html')) decoded = stripHtml(decoded);
  return decoded.trim();
}

function extractFromMultipart(body: string, boundary: string): string {
  const parts = body.split('--' + boundary);
  let plainText = '', htmlText = '';
  for (const part of parts) {
    if (part.startsWith('--') || part.trim().length === 0) continue;
    const headerEnd = part.indexOf('\n\n');
    if (headerEnd < 0) continue;
    const partHeaders = part.slice(0, headerEnd).toLowerCase();
    const partBody = part.slice(headerEnd + 2);
    const partEncoding = partHeaders.match(/content-transfer-encoding:\s*(\S+)/)?.[1] || '';

    const nestedBoundary = partHeaders.match(/boundary="?([^";\s]+)"?/)?.[1];
    if (nestedBoundary) { const nested = extractFromMultipart(partBody, nestedBoundary); if (nested) plainText = nested; continue; }

    let decoded = partBody;
    if (partEncoding.includes('base64')) { try { decoded = Buffer.from(partBody.replace(/\s/g, ''), 'base64').toString('utf-8'); } catch { decoded = partBody; } }
    else if (partEncoding.includes('quoted-printable')) decoded = decodeQuotedPrintable(partBody);

    if (partHeaders.includes('text/plain')) plainText = decoded.trim();
    else if (partHeaders.includes('text/html') && !plainText) htmlText = stripHtml(decoded).trim();
  }
  return plainText || htmlText;
}

function decodeQuotedPrintable(text: string): string {
  return text.replace(/=\r?\n/g, '').replace(/=([0-9A-Fa-f]{2})/g, (_, hex) => String.fromCharCode(parseInt(hex, 16)));
}

function stripHtml(html: string): string {
  return html
    .replace(/<style[^>]*>[\s\S]*?<\/style>/gi, '')
    .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, '')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/(?:p|div|h[1-6]|li|tr)>/gi, '\n')
    .replace(/<[^>]+>/g, '')
    .replace(/&nbsp;/gi, ' ').replace(/&amp;/gi, '&').replace(/&lt;/gi, '<').replace(/&gt;/gi, '>').replace(/&quot;/gi, '"').replace(/&#39;/gi, "'")
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function shouldSkip(labels: string[]): string | null {
  for (const label of labels) {
    const lower = label.toLowerCase().trim();
    if (SKIP_LABELS.has(lower)) return lower;
  }
  return null;
}
