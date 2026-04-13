/**
 * Google OAuth2 helper — reads tokens from google_tokens table.
 */
import { google } from 'googleapis';
import { getPool, createLogger } from '@nexus/core';

const logger = createLogger('google-auth');

export async function getAuthenticatedClient() {
  const clientId = process.env.GOOGLE_CLIENT_ID;
  const clientSecret = process.env.GOOGLE_CLIENT_SECRET;

  if (!clientId || !clientSecret) {
    logger.logMinimal('GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET not set');
    return null;
  }

  const pool = getPool();
  const { rows } = await pool.query(
    `SELECT id, access_token, refresh_token, scope, token_type, expiry_date
     FROM google_tokens ORDER BY created_at DESC LIMIT 1`,
  );

  if (rows.length === 0) return null;

  const tokens = rows[0];
  const oauth2Client = new google.auth.OAuth2(clientId, clientSecret);

  oauth2Client.setCredentials({
    access_token: tokens.access_token,
    refresh_token: tokens.refresh_token,
    scope: tokens.scope,
    token_type: tokens.token_type,
    expiry_date: tokens.expiry_date ? Number(tokens.expiry_date) : undefined,
  });

  oauth2Client.on('tokens', async (newTokens) => {
    try {
      await pool.query(
        `UPDATE google_tokens
         SET access_token = COALESCE($1, access_token),
             refresh_token = COALESCE($2, refresh_token),
             expiry_date = COALESCE($3, expiry_date),
             updated_at = NOW()
         WHERE id = $4`,
        [newTokens.access_token, newTokens.refresh_token, newTokens.expiry_date ?? null, tokens.id],
      );
    } catch (err) {
      logger.logMinimal('Failed to persist refreshed tokens:', (err as Error).message);
    }
  });

  const expired = !tokens.expiry_date || Number(tokens.expiry_date) <= Date.now();
  if (!tokens.access_token || expired) {
    try {
      await oauth2Client.getAccessToken();
    } catch {
      logger.logMinimal('Token refresh failed — Google may need re-authorization');
      return null;
    }
  }

  return oauth2Client;
}
