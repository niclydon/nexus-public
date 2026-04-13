import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';
import { getPool } from '@nexus/core';

export function registerPeopleTools(server: McpServer): void {
  // ── People Lookup ───────────────────────────────────────
  server.tool(
    'nexus_people_lookup',
    'Look up a person in the social identity graph with relationships and communication patterns',
    {
      name: z.string().optional().describe('Fuzzy match on display name'),
      person_id: z.number().optional().describe('Exact person ID'),
      limit: z.number().optional().default(5).describe('Max results for name search'),
    },
    async ({ name, person_id, limit }) => {
      if (!name && !person_id) {
        return { content: [{ type: 'text' as const, text: 'ERROR: Provide name or person_id' }] };
      }
      const pool = getPool();

      // Find person(s)
      let personRows: any[];
      if (person_id) {
        const { rows } = await pool.query(
          `SELECT id, display_name, relationship_type, tier, birthday, birth_year, nickname, notes
           FROM aurora_social_identities WHERE id = $1`,
          [person_id],
        );
        personRows = rows;
      } else {
        const { rows } = await pool.query(
          `SELECT id, display_name, relationship_type, tier, birthday, birth_year, nickname, notes
           FROM aurora_social_identities
           WHERE display_name ILIKE '%' || $1 || '%' AND is_person = true
           ORDER BY tier ASC NULLS LAST
           LIMIT $2`,
          [name, limit],
        );
        personRows = rows;
      }

      if (personRows.length === 0) {
        return { content: [{ type: 'text' as const, text: 'No matching person found.' }] };
      }

      // Enrich each person with links and relationships
      for (const person of personRows) {
        const { rows: links } = await pool.query(
          `SELECT platform, identifier, identifier_type
           FROM aurora_social_identity_links WHERE identity_id = $1`,
          [person.id],
        );
        person.links = links;

        const { rows: relationships } = await pool.query(
          `SELECT pc.person_b_id, asi.display_name as person_b_name,
                  pc.relationship_type, pc.is_inverse
           FROM person_connections pc
           JOIN aurora_social_identities asi ON asi.id = pc.person_b_id
           WHERE pc.person_a_id = $1
           LIMIT 20`,
          [person.id],
        );
        person.relationships = relationships;

        // Recent communication count
        const { rows: commCount } = await pool.query(
          `SELECT COUNT(*)::int as count
           FROM aurora_unified_communication
           WHERE identity_id = $1 AND timestamp > NOW() - INTERVAL '30 days'`,
          [person.id],
        );
        person.recent_messages_30d = commCount[0]?.count ?? 0;
      }

      return { content: [{ type: 'text' as const, text: JSON.stringify({ people: personRows }, null, 2) }] };
    },
  );

  // ── Contacts Search ─────────────────────────────────────
  server.tool(
    'nexus_contacts_search',
    'Search contacts by name, phone, or email',
    {
      query: z.string().describe('Search term (matches name, phone, email)'),
      limit: z.number().optional().default(10).describe('Max results'),
    },
    async ({ query, limit }) => {
      const pool = getPool();
      const { rows } = await pool.query(
        `SELECT id, display_name, job_title, company,
                phone_numbers, email_addresses, notes, updated_at
         FROM contacts
         WHERE display_name ILIKE '%' || $1 || '%'
            OR phone_numbers::text ILIKE '%' || $1 || '%'
            OR email_addresses::text ILIKE '%' || $1 || '%'
         ORDER BY display_name
         LIMIT $2`,
        [query, limit],
      );
      return { content: [{ type: 'text' as const, text: JSON.stringify(rows, null, 2) }] };
    },
  );
}
