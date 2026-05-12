import { describe, test, expect } from "bun:test";
import { Job as JobProto, JobKind, JobStatus } from "../gen/gastrolog/v1/job_pb";
import { Job } from "./job";
import { idFromBytes } from "./id";

function bytes(b: number): Uint8Array<ArrayBuffer> {
  const out = new Uint8Array(new ArrayBuffer(16));
  out[0] = b;
  return out;
}

function mkJob(overrides: Partial<JobProto>): Job {
  return new Job(new JobProto({ id: bytes(1), name: "x", ...overrides }));
}

describe("Job kind predicates", () => {
  test("isTask / isScheduled", () => {
    expect(mkJob({ kind: JobKind.TASK }).isTask).toBe(true);
    expect(mkJob({ kind: JobKind.SCHEDULED }).isScheduled).toBe(true);
    expect(mkJob({ kind: JobKind.TASK }).isScheduled).toBe(false);
    expect(mkJob({ kind: JobKind.UNSPECIFIED }).isTask).toBe(false);
  });
});

describe("Job status predicates", () => {
  test("isPending / isRunning / isCompleted / isFailed", () => {
    expect(mkJob({ status: JobStatus.PENDING }).isPending).toBe(true);
    expect(mkJob({ status: JobStatus.RUNNING }).isRunning).toBe(true);
    expect(mkJob({ status: JobStatus.COMPLETED }).isCompleted).toBe(true);
    expect(mkJob({ status: JobStatus.FAILED }).isFailed).toBe(true);
  });

  test("isFinished covers completed and failed", () => {
    expect(mkJob({ status: JobStatus.COMPLETED }).isFinished).toBe(true);
    expect(mkJob({ status: JobStatus.FAILED }).isFinished).toBe(true);
    expect(mkJob({ status: JobStatus.RUNNING }).isFinished).toBe(false);
    expect(mkJob({ status: JobStatus.PENDING }).isFinished).toBe(false);
  });

  test("isActive covers pending and running", () => {
    expect(mkJob({ status: JobStatus.PENDING }).isActive).toBe(true);
    expect(mkJob({ status: JobStatus.RUNNING }).isActive).toBe(true);
    expect(mkJob({ status: JobStatus.COMPLETED }).isActive).toBe(false);
  });
});

describe("Job statusLabel + statusVariant", () => {
  test("labels match the badge text", () => {
    expect(mkJob({ status: JobStatus.PENDING }).statusLabel).toBe("pending");
    expect(mkJob({ status: JobStatus.RUNNING }).statusLabel).toBe("running");
    expect(mkJob({ status: JobStatus.COMPLETED }).statusLabel).toBe("completed");
    expect(mkJob({ status: JobStatus.FAILED }).statusLabel).toBe("failed");
    expect(mkJob({ status: JobStatus.UNSPECIFIED }).statusLabel).toBe("");
  });

  test("variants match the existing badge mapping", () => {
    expect(mkJob({ status: JobStatus.RUNNING }).statusVariant).toBe("info");
    expect(mkJob({ status: JobStatus.COMPLETED }).statusVariant).toBe("copper");
    expect(mkJob({ status: JobStatus.FAILED }).statusVariant).toBe("error");
    expect(mkJob({ status: JobStatus.PENDING }).statusVariant).toBe("muted");
  });
});

describe("Job display + ids", () => {
  test("displayLabel: description → name → id", () => {
    expect(mkJob({ description: "d", name: "n" }).displayLabel).toBe("d");
    expect(mkJob({ description: "", name: "n" }).displayLabel).toBe("n");
    const idOnly = mkJob({ description: "", name: "" });
    expect(idOnly.displayLabel).toBe(idOnly.id);
  });

  test("nodeId converts to EntityID", () => {
    const j = mkJob({ nodeId: bytes(7) });
    expect(j.nodeId).toBe(idFromBytes(bytes(7)));
  });
});

describe("Job progress helpers", () => {
  test("hasProgress is true when chunks or records have advanced", () => {
    expect(mkJob({ chunksTotal: 5n }).hasProgress).toBe(true);
    expect(mkJob({ recordsDone: 10n }).hasProgress).toBe(true);
    expect(mkJob({}).hasProgress).toBe(false);
  });

  test("hasProgressSurface is true for running and finished states", () => {
    expect(mkJob({ status: JobStatus.RUNNING }).hasProgressSurface).toBe(true);
    expect(mkJob({ status: JobStatus.COMPLETED }).hasProgressSurface).toBe(true);
    expect(mkJob({ status: JobStatus.FAILED }).hasProgressSurface).toBe(true);
    expect(mkJob({ status: JobStatus.PENDING }).hasProgressSurface).toBe(false);
  });
});
