/**
 * E.164 phone number normalization.
 *
 * Strips formatting (spaces, dashes, parens, dots) and ensures
 * US numbers have +1 prefix. Used at ingestion time to ensure
 * consistent identifier matching across all data sources.
 */

/**
 * Normalize a phone number to E.164 format.
 * Returns null for non-phone identifiers (emails, chat IDs, short codes).
 */
export function normalizePhone(identifier: string): string {
  if (!identifier) return identifier;

  // Skip emails and non-phone identifiers
  if (identifier.includes('@') || identifier.startsWith('chat')) return identifier;

  // Strip formatting characters
  let cleaned = identifier.replace(/[\s\-\(\)\.]/g, '');

  // Skip short codes and vanity numbers
  if (cleaned.length < 7 || /[a-zA-Z]/.test(cleaned)) return identifier;

  // Add +1 for bare 10-digit US numbers
  if (/^[2-9]\d{9}$/.test(cleaned)) {
    cleaned = '+1' + cleaned;
  }

  // Add + for 11-digit numbers starting with 1 (US with country code but no +)
  if (/^1[2-9]\d{9}$/.test(cleaned)) {
    cleaned = '+' + cleaned;
  }

  // Ensure international numbers have + prefix
  if (/^\d{10,15}$/.test(cleaned) && !cleaned.startsWith('+')) {
    cleaned = '+' + cleaned;
  }

  return cleaned;
}
