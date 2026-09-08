import { appendFileSync, mkdirSync } from "node:fs";
import { dirname } from "node:path";
import type {
  MockTransportCall,
  TransportContext,
  TransportResult,
  WakePayload,
  WakeTransport,
} from "./types.js";

/** In-memory transport used by tests and local demos. */
export class MockTransport implements WakeTransport {
  readonly kind = "mock";
  readonly calls: MockTransportCall[] = [];
  private failuresRemaining = 0;
  private readonly acceptedBatches = new Set<string>();

  failNext(count = 1): void {
    this.failuresRemaining = Math.max(0, Math.floor(count));
  }

  dispatch(context: TransportContext): TransportResult {
    const call: MockTransportCall = {
      attempt_id: context.attempt_id,
      batch_id: context.batch.id,
      endpoint_id: context.endpoint.id,
      generation: context.binding.generation,
      payload: context.payload,
      at: new Date().toISOString(),
    };
    this.calls.push(call);
    if (this.failuresRemaining > 0) {
      this.failuresRemaining -= 1;
      return {
        accepted: false,
        retryable: true,
        error_class: "mock_failure",
        error_message: "mock transport configured to fail",
      };
    }
    this.acceptedBatches.add(context.batch.id);
    return {
      accepted: true,
      transport_kind: this.kind,
      receipt_details: { deduplicated: false },
    };
  }

  hasAccepted(batchId: string): boolean {
    return this.acceptedBatches.has(batchId);
  }
}

/**
 * File transport is intentionally boring: one JSON envelope per line.  It is
 * useful as a scriptable local adapter and never evaluates the envelope as a
 * shell command or copies source正文 into it.
 */
export class FileTransport implements WakeTransport {
  readonly kind = "file";

  constructor(readonly filePath: string) {
    mkdirSync(dirname(filePath), { recursive: true });
  }

  dispatch(context: TransportContext): TransportResult {
    const line = JSON.stringify({
      attempt_id: context.attempt_id,
      batch_id: context.batch.id,
      endpoint_id: context.endpoint.id,
      generation: context.binding.generation,
      payload: context.payload,
      accepted_at: new Date().toISOString(),
    });
    appendFileSync(this.filePath, `${line}\n`, { encoding: "utf8", mode: 0o600 });
    return {
      accepted: true,
      transport_kind: this.kind,
      receipt_details: { path: this.filePath },
    };
  }
}

export type TransportFactory = (payload: WakePayload) => WakeTransport;

