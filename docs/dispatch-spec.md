# Dispatch Spec

**Status:** active delivery contract

Dispatch Spec is the contract for human-facing delivery surfaces in Nexus. A
delivery surface is more than a handler name: it needs a clear lane, registry
coverage, and an explicit role in the platform.

## Delivery Lanes

The public repo currently exposes these delivery concepts:

- Immediate interrupt: push-style notifications for urgent events.
- Inbox/archive: durable records written to the platform inbox/report surfaces.
- Scheduled reports: generated summaries routed through `report_subscriptions`.
- Email delivery: explicit outbound delivery methods, not a catch-all default.
- Internal coordination: agent-to-agent inbox traffic, which is not a human
  delivery lane.

## Registry Expectations

Delivery methods are configuration-backed. In practice that means:

- `notification_delivery_methods` defines available channels and per-channel
  config.
- `report_subscriptions` decides which report types are enabled and where they
  go.
- job handlers such as `insight-deliver`, `push-notification`, and
  `aria-email-send` provide runtime coverage for configured channels.

Adding a new delivery surface should update all three layers together:

1. configuration or schema support,
2. runtime handler coverage,
3. documentation of the lane and its intended use.

## Guardrails

- Human delivery should have a named lane and purpose.
- Agent coordination should not be treated as human notification.
- Delivery configuration should not silently point to channels that have no
  handler support.
- Report delivery should not duplicate archive writes just because multiple
  channels are configured.

## Practical Rule

If a new notification path is worth shipping, it should be discoverable in the
same places as the existing ones: schema/config, handler registration, and
docs. One-off side channels are exactly the drift this contract is meant to
prevent.
