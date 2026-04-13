/**
 * Reverse geocoding — Google Geocoding API primary, Nominatim fallback.
 * Results cached in memory and geocode_cache table.
 */
import { getPool, createLogger } from '@nexus/core';

const logger = createLogger('geocode');

const memoryCache = new Map<string, string>();

function cacheKey(lat: number, lon: number): string {
  return `${lat.toFixed(2)},${lon.toFixed(2)}`;
}

const GOOGLE_GEOCODE_URL = 'https://maps.googleapis.com/maps/api/geocode/json';

function formatGoogleAddress(components: Array<{ long_name: string; short_name: string; types: string[] }>): string {
  const parts: string[] = [];
  const locality = components.find(c => c.types.includes('locality'));
  const sublocality = components.find(c => c.types.includes('sublocality'));
  const adminL1 = components.find(c => c.types.includes('administrative_area_level_1'));
  const country = components.find(c => c.types.includes('country'));
  const place = locality?.long_name || sublocality?.long_name;
  if (place) parts.push(place);
  if (adminL1 && adminL1.long_name !== place) parts.push(adminL1.long_name);
  if (country) parts.push(country.long_name);
  return parts.join(', ') || 'Unknown location';
}

async function reverseGeocodeGoogle(lat: number, lon: number): Promise<string | null> {
  const apiKey = process.env.GOOGLE_GENAI_API_KEY;
  if (!apiKey) return null;
  try {
    const url = `${GOOGLE_GEOCODE_URL}?latlng=${lat},${lon}&key=${apiKey}&result_type=locality|administrative_area_level_1`;
    const response = await fetch(url, { signal: AbortSignal.timeout(5000) });
    if (!response.ok) return null;
    const data = await response.json() as { status: string; results?: Array<{ address_components: Array<{ long_name: string; short_name: string; types: string[] }> }> };
    if (data.status !== 'OK' || !data.results?.length) return null;
    return formatGoogleAddress(data.results[0].address_components);
  } catch { return null; }
}

const NOMINATIM_URL = 'https://nominatim.openstreetmap.org/reverse';
let lastNominatimRequest = 0;

async function reverseGeocodeNominatim(lat: number, lon: number): Promise<string | null> {
  try {
    const elapsed = Date.now() - lastNominatimRequest;
    if (elapsed < 1100) await new Promise(r => setTimeout(r, 1100 - elapsed));
    lastNominatimRequest = Date.now();

    const url = `${NOMINATIM_URL}?lat=${lat}&lon=${lon}&format=json&addressdetails=1&zoom=10`;
    const response = await fetch(url, {
      headers: { 'User-Agent': 'ARIA/1.0 (personal assistant, single-user)' },
      signal: AbortSignal.timeout(10000),
    });
    if (!response.ok) return null;
    const data = await response.json() as { display_name?: string; address?: Record<string, string> };
    if (!data.address) return data.display_name || null;
    const parts: string[] = [];
    const locality = data.address.city || data.address.town || data.address.village || data.address.hamlet;
    if (locality) parts.push(locality);
    const region = data.address.state || data.address.province;
    if (region && region !== locality) parts.push(region);
    if (data.address.country) parts.push(data.address.country);
    return parts.join(', ') || 'Unknown location';
  } catch { return null; }
}

export async function reverseGeocode(lat: number, lon: number): Promise<string | null> {
  const key = cacheKey(lat, lon);
  if (memoryCache.has(key)) return memoryCache.get(key)!;

  try {
    const pool = getPool();
    const { rows } = await pool.query<{ location_name: string }>(
      `SELECT location_name FROM geocode_cache WHERE cache_key = $1`, [key],
    );
    if (rows.length > 0) { memoryCache.set(key, rows[0].location_name); return rows[0].location_name; }
  } catch { /* miss */ }

  let locationName = await reverseGeocodeGoogle(lat, lon);
  if (!locationName) locationName = await reverseGeocodeNominatim(lat, lon);
  if (!locationName) return null;

  memoryCache.set(key, locationName);
  try {
    const pool = getPool();
    await pool.query(
      `INSERT INTO geocode_cache (cache_key, latitude, longitude, location_name)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (cache_key) DO UPDATE SET location_name = $4, updated_at = NOW()`,
      [key, Number(lat.toFixed(2)), Number(lon.toFixed(2)), locationName],
    );
  } catch { /* non-fatal */ }

  return locationName;
}

export async function batchReverseGeocode(coordinates: Array<{ lat: number; lon: number }>): Promise<Map<string, string>> {
  const results = new Map<string, string>();
  const unique = new Map<string, { lat: number; lon: number }>();
  for (const coord of coordinates) {
    const key = cacheKey(coord.lat, coord.lon);
    if (!unique.has(key)) unique.set(key, coord);
  }
  for (const [key, coord] of Array.from(unique.entries())) {
    const name = await reverseGeocode(coord.lat, coord.lon);
    if (name) results.set(key, name);
  }
  return results;
}

export { cacheKey as geocodeCacheKey };
