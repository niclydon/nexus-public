// Smoke test for the public showcase guard (packages/core/src/showcase.ts).
// Runs against the compiled output: `npm test` builds first, then runs this.
// No database or network required.
import assert from 'node:assert/strict';

// Import from the built core package.
const showcase = await import(new URL('../packages/core/dist/showcase.js', import.meta.url));

// Guard is OFF unless the demo flag is set: services must refuse to start.
delete process.env.NEXUS_SHOWCASE_DEMO;
delete process.env.NEXUS_SHOWCASE_ALLOW_DANGEROUS_TOOLS;
assert.equal(showcase.isShowcaseDemoEnabled(), false);
assert.equal(showcase.isShowcaseDangerousToolsAllowed(), false);
assert.throws(() => showcase.assertShowcaseDemoEnabled('smoke'), /showcase-only/);

// Demo on: startup allowed, dangerous tools still off by default.
process.env.NEXUS_SHOWCASE_DEMO = 'true';
assert.equal(showcase.isShowcaseDemoEnabled(), true);
showcase.assertShowcaseDemoEnabled('smoke');
assert.equal(showcase.isShowcaseDangerousToolsAllowed(), false);

// Dangerous-tools flag parses truthy values only.
process.env.NEXUS_SHOWCASE_ALLOW_DANGEROUS_TOOLS = 'true';
assert.equal(showcase.isShowcaseDangerousToolsAllowed(), true);
process.env.NEXUS_SHOWCASE_ALLOW_DANGEROUS_TOOLS = 'false';
assert.equal(showcase.isShowcaseDangerousToolsAllowed(), false);

console.log('smoke-test: showcase guard OK');
