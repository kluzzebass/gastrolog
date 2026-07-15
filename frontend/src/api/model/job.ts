// Job domain model.
//
// Wraps the wire `Job` proto with typed predicates so component code stops
// switch/case-ing on `JobKind` / `JobStatus` enum members. Mirrors the
// pattern set by Ingester / Vault / Node.

import type { Job as JobProto } from "../gen/gastrolog/v1/job_pb";
import { JobKind, JobStatus } from "../gen/gastrolog/v1/job_pb";
import type { Timestamp } from "@bufbuild/protobuf";
import { type EntityID, idFromBytes } from "./id";
import { formatJobSchedule } from "../../utils/jobSchedule";

/** Badge variant used for the per-job status pill. */
export type JobStatusVariant = "muted" | "info" | "copper" | "error";

export class Job {
  readonly id: EntityID;
  readonly nodeId: EntityID;
  readonly name: string;
  readonly description: string;
  readonly kind: JobKind;
  readonly status: JobStatus;
  readonly chunksTotal: bigint;
  readonly chunksDone: bigint;
  readonly recordsDone: bigint;
  readonly error: string;
  readonly errorDetails: readonly string[];
  readonly schedule: string;
  readonly startedAt: Timestamp | undefined;
  readonly completedAt: Timestamp | undefined;
  readonly lastRun: Timestamp | undefined;
  readonly nextRun: Timestamp | undefined;

  constructor(proto: JobProto) {
    this.id = idFromBytes(proto.id);
    this.nodeId = idFromBytes(proto.nodeId);
    this.name = proto.name;
    this.description = proto.description;
    this.kind = proto.kind;
    this.status = proto.status;
    this.chunksTotal = proto.chunksTotal;
    this.chunksDone = proto.chunksDone;
    this.recordsDone = proto.recordsDone;
    this.error = proto.error;
    this.errorDetails = proto.errorDetails;
    this.schedule = proto.schedule;
    this.startedAt = proto.startedAt;
    this.completedAt = proto.completedAt;
    this.lastRun = proto.lastRun;
    this.nextRun = proto.nextRun;
  }

  /** Description → name → id (tasks and expandable headers). */
  get displayLabel(): string {
    return this.description || this.name || this.id;
  }

  /** Short cron-job row label — name only; see help for what each job does. */
  get scheduleLabel(): string {
    return this.name || this.id;
  }

  /** Canonical 6-field cron for inspector display. */
  get displaySchedule(): string {
    return formatJobSchedule(this.schedule);
  }

  get isTask(): boolean {
    return this.kind === JobKind.TASK;
  }

  get isScheduled(): boolean {
    return this.kind === JobKind.SCHEDULED;
  }

  get isPending(): boolean {
    return this.status === JobStatus.PENDING;
  }

  get isRunning(): boolean {
    return this.status === JobStatus.RUNNING;
  }

  get isCompleted(): boolean {
    return this.status === JobStatus.COMPLETED;
  }

  get isFailed(): boolean {
    return this.status === JobStatus.FAILED;
  }

  /** Terminal states: completed or failed. */
  get isFinished(): boolean {
    return this.isCompleted || this.isFailed;
  }

  /** Pre-terminal states: pending or running. */
  get isActive(): boolean {
    return this.isPending || this.isRunning;
  }

  get statusLabel(): string {
    switch (this.status) {
      case JobStatus.PENDING: return "pending";
      case JobStatus.RUNNING: return "running";
      case JobStatus.COMPLETED: return "completed";
      case JobStatus.FAILED: return "failed";
      default: return "";
    }
  }

  get statusVariant(): JobStatusVariant {
    switch (this.status) {
      case JobStatus.RUNNING: return "info";
      case JobStatus.COMPLETED: return "copper";
      case JobStatus.FAILED: return "error";
      default: return "muted";
    }
  }

  /** True when the job has either chunk progress or record progress to show. */
  get hasProgress(): boolean {
    return this.chunksTotal > 0n || this.recordsDone > 0n;
  }

  /** True when progress fields are meaningful (running / completed / failed). */
  get hasProgressSurface(): boolean {
    return this.isRunning || this.isFinished;
  }
}
