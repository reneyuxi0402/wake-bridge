/** Error raised by a public contract validator or SDK client. */
export class WakeBridgeSdkError extends Error {
  constructor(
    message: string,
    readonly code: string = "invalid_contract",
    readonly status: number = 400,
    readonly remote_code?: string,
  ) {
    super(message);
    this.name = "WakeBridgeSdkError";
  }
}
