import type { Server } from "node:http";

export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };

export type PriorityHint = "low" | "normal" | "high" | "urgent" | string;
export type EventState = "received" | "matched" | "suppressed" | "batched" | "consumed" | "expired";
export type ClaimOrigin = "policy" | "self_commitment" | "continuity_watch" | "inactivity_watch";
export type ClaimState = "pending" | "deferred" | "eligible" | "batched" | "consumed" | "dismissed" | "expired";
export type DeliveryMode = "immediate" | "scheduled" | "suppress";
export type QuietHoursPolicy = "defer" | "bypass";
export type ForegroundPresencePolicy = "defer" | "bypass";
export type BatchState =
  | "pending"
  | "waiting_for_endpoint"
  | "waiting_for_waiter"
  | "dispatching"
  | "retry_wait"
  | "dispatched"
  | "seen"
  | "cancelled"
  | "needs_attention"
  | "dead_letter";
export type AttemptState = "leased" | "accepted" | "failed" | "expired";
export type ReceiptStage = "transport_accepted" | "agent_completed" | "agent_seen" | "agent_consumed" | "agent_acted";
export type SourceReadSideEffects = "none" | "marks_read" | "consumes" | "unknown";
export type SourceCredentialCustody = "none" | "connector" | "wake_bridge_process";
export type SourceUpstreamCredentialBreadth = "none" | "read_only" | "broad" | "unknown";
export type SourceCursor = JsonValue | null;

export interface ResourceRef {
  uri: string;
  cursor?: string | number | null;
  [key: string]: unknown;
}

export interface WakeEventInput {
  schema_version?: number;
  type: string;
  occurred_at?: string;
  dedupe_key: string;
  coalesce_key?: string | null;
  priority_hint?: PriorityHint | null;
  attention_channel_hint?: string | null;
  actor_ref?: string | null;
  resource: ResourceRef;
  metadata?: Record<string, JsonValue>;
  payload_preview?: string | null;
  /** Optional caller idempotency key. It never changes source dedupe semantics. */
  idempotency_key?: string | null;
}

/** Public capability declaration for one pull source adapter. */
export interface SourceAdapterManifest {
  /** Public source SDK contract, independent from this adapter's release version. */
  contract_version: 1;
  id: string;
  version: string;
  /** Non-secret upstream principal chosen and confirmed by the source owner. */
  subject_ref: string;
  /** Non-secret sha256 fence for the connector's current upstream configuration. */
  binding_fingerprint: string;
  read_side_effects: SourceReadSideEffects;
  /** Where the provider credential is actually held, not who owns the account. */
  credential_custody: SourceCredentialCustody;
  /** Honest breadth of the provider credential, which may be broader than this connector surface. */
  upstream_credential_breadth: SourceUpstreamCredentialBreadth;
  upstream_credential_scopes: string[];
  /** The narrow operations exposed by the connector to its Wake source adapter. */
  connector_capabilities: string[];
}

export interface SourcePollContext {
  cursor: SourceCursor;
  limit: number;
}

export interface SourcePollResult {
  events: WakeEventInput[];
  next_cursor: SourceCursor;
  /** True asks the runner to schedule another bounded pass soon. */
  has_more?: boolean;
}

export interface PullSourceAdapter {
  readonly manifest: SourceAdapterManifest;
  poll(context: SourcePollContext): Promise<SourcePollResult> | SourcePollResult;
}

export interface SourceCheckpoint {
  instance_id: string;
  source: string;
  adapter_version: string;
  subject_ref: string;
  binding_fingerprint: string;
  cursor: SourceCursor;
  revision: number;
  updated_at: string;
}

/** Agent-facing checkpoint metadata. Opaque provider cursors stay inside Core. */
export type SourceCheckpointSummary = Omit<SourceCheckpoint, "cursor" | "instance_id">;

export interface SourceRunResult {
  source: string;
  adapter_version: string;
  checkpoint_revision: number;
  events_seen: number;
  events_inserted: number;
  events_duplicate: number;
  has_more: boolean;
}

/** Owner-controlled local connector. The token authenticates to the connector, not the provider. */
export interface SourceConnectorConfig {
  id: string;
  base_url: string;
  token_env: string;
  enabled?: boolean;
  poll_interval_ms?: number;
  max_backoff_ms?: number;
  limit?: number;
  max_pages_per_cycle?: number;
}

export interface SourceConnectorFile {
  version: 1;
  sources: SourceConnectorConfig[];
}

export type SourceSupervisorState = "idle" | "polling" | "healthy" | "backoff" | "needs_attention" | "stopped";

export interface SourceSupervisorStatus {
  source: string;
  enabled: boolean;
  state: SourceSupervisorState;
  last_started_at: string | null;
  last_success_at: string | null;
  last_error_at: string | null;
  last_error_class: string | null;
  consecutive_failures: number;
  next_poll_at: string | null;
  checkpoint_revision: number;
  checkpoint_subject_ref: string | null;
  checkpoint_binding_fingerprint: string | null;
  last_run: SourceRunResult | null;
}

export interface SourceVerification {
  source: string;
  manifest: SourceAdapterManifest;
  checkpoint: SourceCheckpointSummary | null;
  binding_matches: boolean;
  enabled: boolean;
}

export interface WakeEvent extends WakeEventInput {
  id: string;
  schema_version: number;
  instance_id: string;
  owner_id: string;
  source: string;
  occurred_at: string;
  received_at: string;
  state: EventState;
  matched_policy_id?: string | null;
  matched_policy_version?: number | null;
}

export interface PolicyMatch {
  source?: string;
  type?: string;
  actor_ref?: string;
  attention_channel_hint?: string;
  metadata?: Record<string, JsonValue | undefined>;
  /** Dot-path selectors are also accepted for convenient JSON policy files. */
  [key: string]: unknown;
}

export interface PolicyDelivery {
  mode: DeliveryMode;
  scheduled_local_time?: string | null;
  quiet_hours_policy?: QuietHoursPolicy;
  foreground_presence_policy?: ForegroundPresencePolicy;
}

export interface PolicyBatch {
  coalesce_by?: "coalesce_key" | "type" | "source" | "none" | string;
  max_events?: number | null;
  window_ms?: number | null;
}

export interface PolicyTarget {
  attention_channel?: string | null;
}

export interface PolicyRule {
  id: string;
  version: number;
  enabled?: boolean;
  order?: number;
  match?: PolicyMatch;
  delivery: PolicyDelivery;
  batch?: PolicyBatch;
  target?: PolicyTarget;
  reason_code?: string;
  expires_after_ms?: number | null;
}

/**
 * Read-only representation of policy rows created before public policy v1.
 * Legacy fields are never accepted by current install surfaces and legacy
 * rules do not participate in matching new events.
 */
export interface InstalledPolicyRule extends Omit<PolicyRule, "delivery" | "target"> {
  delivery: Omit<PolicyDelivery, "mode"> & {
    mode: DeliveryMode | "debounce" | "digest";
    debounce_ms?: number | null;
    cadence_ms?: number | null;
    max_delay_ms?: number | null;
  };
  target?: PolicyTarget & {
    prefer_transport?: "warm" | "cold" | "any";
    busy_behavior?: "queue" | "retry" | "steer";
  };
}

/** Stable, owner-authored policy file contract. */
export interface PolicyFile {
  schema_version: 1;
  policies: PolicyRule[];
}

export interface PolicyMatchResult {
  source: string;
  type: string;
  matched_policy: PolicyRule;
}

export interface PolicyPreviewResult extends PolicyMatchResult {
  evaluated_at: string;
  suppressed: boolean;
  claim: null | {
    attention_channel: string;
    eligible_after: string;
    expires_at: string | null;
    reason_code: string;
    defer_while_presence: boolean;
  };
  gates: {
    quiet_hours: { action: QuietHoursPolicy; active: boolean; defer_until: string | null };
    foreground_presence: { action: ForegroundPresencePolicy; active: boolean; defer_until: string | null };
  };
  batch: null | {
    coalesce_by: string;
    coalesce_key: string | null;
    max_events: number;
    window_ms: number;
  };
}

export interface PolicyVersionStatus {
  id: string;
  version: number;
  active: boolean;
  enabled: boolean;
  new_event_matching: "eligible" | "disabled" | "legacy_unsupported" | "historical";
}

export interface QuietHoursWindow {
  start: string;
  end: string;
}

export interface QuietHoursConfig {
  timezone?: string;
  windows: QuietHoursWindow[];
}

export interface BridgeConfig {
  instance_id: string;
  owner_id: string;
  db_path: string;
  timezone?: string;
  admin_token?: string;
  quiet_hours?: QuietHoursConfig | null;
  presence_ttl_ms?: number;
  activity_ttl_ms?: number;
  endpoint_lease_ms?: number;
  dispatch_lease_ms?: number;
  retry_delay_ms?: number;
  batch_window_ms?: number;
  default_policy?: PolicyRule;
  policies?: PolicyRule[];
}

export interface AttentionClaim {
  id: string;
  instance_id: string;
  origin: ClaimOrigin;
  event_ids: string[];
  resource: ResourceRef;
  policy_id: string;
  policy_version: number;
  attention_channel: string;
  eligible_after: string;
  expires_at?: string | null;
  defer_while_presence: boolean;
  state: ClaimState;
  reason_code: string;
  note?: string | null;
  created_at: string;
  updated_at: string;
  snooze_until?: string | null;
  consumed_result?: JsonValue | null;
  dismissed_reason?: string | null;
}

export interface EndpointRoute {
  kind: string;
  priority?: number;
  address: Record<string, JsonValue>;
}

export interface EndpointCapabilities {
  cold_push?: boolean;
  warm_resume?: boolean;
  exact_live_route?: boolean;
  requires_live_binding?: boolean;
  queue_when_busy?: boolean;
  steer_when_busy?: boolean;
  foreground_presence_observable?: boolean;
  session_activity_observable?: boolean;
  activity_progress_observable?: boolean;
  waiter_turn_settle_compatible?: boolean;
  session_rebuild_takeover?: boolean;
  receipt_upper_bound?: string;
  support_tier?: string;
  receipt_stages?: ReceiptStage[];
  [key: string]: JsonValue | undefined;
}

export interface EndpointRegistration {
  id?: string;
  host_kind: string;
  session_ref: string;
  capabilities?: EndpointCapabilities;
  routes?: EndpointRoute[];
  lease_ms?: number;
}

export interface Endpoint {
  id: string;
  instance_id: string;
  host_kind: string;
  session_ref: string;
  lease_token?: string;
  lease_expires_at: string;
  capabilities: EndpointCapabilities;
  routes: EndpointRoute[];
  registered_at: string;
  updated_at: string;
}

export interface Binding {
  instance_id: string;
  attention_channel: string;
  endpoint_id: string;
  generation: number;
  bound_at: string;
}

export interface PresenceLease {
  instance_id: string;
  attention_channel: string;
  endpoint_id: string;
  binding_generation: number;
  renewed_at: string;
  expires_at: string;
  observed_by: string;
  observation: string;
}

export interface PresenceRenewal {
  attention_channel: string;
  endpoint_id: string;
  generation: number;
  ttl_ms?: number;
  lease_token?: string;
  observed_by: string;
  observation?: string;
}

export type ActivityKind =
  | "owner_message_accepted"
  | "activity_started"
  | "activity_progress"
  | "activity_settled"
  | "wake_started"
  | "wake_progress"
  | "wake_settled";

export interface ActivityObservation {
  attention_channel: string;
  endpoint_id: string;
  generation: number;
  lease_token?: string;
  observation_id: string;
  kind: ActivityKind;
  observed_at?: string;
  ttl_ms?: number;
}

export interface ActivityObservationResult {
  duplicate: boolean;
  watch: ActivityWatch | null;
}

export interface ActivityWatchConfiguration {
  attention_channel: string;
  enabled: boolean;
  idle_after_ms: number;
  mode?: "once" | "repeat";
  repeat_after_ms?: number;
}

export interface ActivityWatch {
  instance_id: string;
  attention_channel: string;
  enabled: boolean;
  idle_after_ms: number;
  endpoint_id: string | null;
  binding_generation: number | null;
  epoch: number;
  armed_at: string | null;
  last_nonwake_activity_at: string | null;
  activity_lease_expires_at: string | null;
  fired_epoch: number | null;
  mode: "once" | "repeat";
  repeat_after_ms: number;
  sequence: number;
  last_triggered_at: string | null;
  next_due_at: string | null;
  condition_status: "disabled" | "waiting_for_endpoint" | "unsupported" | "waiting_for_activity" | "watching" | "inactive";
  limits: { min_interval_ms: number; max_claims_per_tick: number };
  updated_at: string;
}

export interface WakeBatch {
  id: string;
  instance_id: string;
  policy_id: string;
  policy_version: number;
  attention_channel: string;
  claim_ids: string[];
  event_ids: string[];
  coalesce_key?: string | null;
  state: BatchState;
  not_before: string;
  deadline?: string | null;
  attempt: number;
  binding_generation?: number | null;
  created_at: string;
  updated_at: string;
  lease_expires_at?: string | null;
  last_error?: string | null;
}

export interface WakeBriefClaim {
  claim_id: string;
  event_id?: string;
  source: string;
  type: string;
  resource: ResourceRef;
  origin: ClaimOrigin;
  reason_code: string;
}

export interface WakePayload {
  schema_version: number;
  instance_id: string;
  wake_batch_id: string;
  attention_channel: string;
  claim_refs: WakeBriefClaim[];
  binding_generation: number;
}

export interface TransportContext {
  batch: WakeBatch;
  endpoint: Endpoint;
  binding: Binding;
  payload: WakePayload;
  attempt_id: string;
  /** One-shot nonce generated and persisted by Core before transport dispatch. */
  delivery_nonce: string;
}

export type DeliveryCorrelationState = "outstanding" | "consumed" | "discarded" | "expired";

export interface DeliveryCorrelation {
  attempt_id: string;
  batch_id: string;
  endpoint_id: string;
  binding_generation: number;
  transport_kind: string;
  nonce_hash: string;
  state: DeliveryCorrelationState;
  created_at: string;
  expires_at: string;
  consumed_at?: string | null;
}

export interface WakeEchoObservation {
  attention_channel: string;
  endpoint_id: string;
  generation: number;
  lease_token?: string;
  observation_id: string;
  delivery_nonce: string;
  observed_at?: string;
  ttl_ms?: number;
}

export interface TransportResult {
  accepted: boolean;
  transport_kind?: string;
  receipt_details?: Record<string, JsonValue>;
  retryable?: boolean;
  /** Optional transport-directed delay before retrying this durable batch. */
  retry_after_ms?: number;
  error_class?: string;
  error_message?: string;
}

export interface WakeTransport {
  readonly kind: string;
  readonly capabilities?: EndpointCapabilities;
  dispatch(context: TransportContext): Promise<TransportResult> | TransportResult;
}

export interface MockTransportCall {
  attempt_id: string;
  batch_id: string;
  endpoint_id: string;
  generation: number;
  payload: WakePayload;
  at: string;
}

export interface Receipt {
  id: string;
  batch_id: string;
  claim_id?: string | null;
  stage: ReceiptStage;
  at: string;
  endpoint_id?: string | null;
  binding_generation?: number | null;
  transport_kind?: string | null;
  details?: Record<string, JsonValue> | null;
}

export interface TickResult {
  inactivity_claims_created: string[];
  expired_claims: string[];
  deferred_claims: string[];
  eligible_claims: string[];
  batches_created: string[];
  batches_updated: string[];
}

export interface DispatchResult {
  batch_id: string;
  status: "accepted" | "waiting_for_endpoint" | "retry_wait" | "needs_attention" | "dead_letter" | "stale_generation" | "skipped";
  attempt_id?: string;
  endpoint_id?: string;
  generation?: number;
  error_class?: string;
}

export interface DaemonHandle {
  server: Server;
  close(): Promise<void>;
}
