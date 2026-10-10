import { describe, expect, test } from "bun:test";
import { render } from "@testing-library/react";
import { Timestamp } from "@bufbuild/protobuf";
import { Job as JobProto, JobKind } from "../../api/gen/gastrolog/v1/job_pb";
import { Job } from "../../api/model/job";
import { COUNTDOWN_MAX_CHARS, ELAPSED_MAX_CHARS } from "../../utils/temporal";
import { ChunkProgress, ScheduledJobsTable } from "./JobCard";

function id(b: number): Uint8Array<ArrayBuffer> {
  const out = new Uint8Array(new ArrayBuffer(16));
  out[0] = b;
  return out;
}

function scheduledJob(): Job {
  const now = Date.now();
  return new Job(
    new JobProto({
      id: id(1),
      name: "retention",
      kind: JobKind.SCHEDULED,
      schedule: "0 * * * * *",
      lastRun: Timestamp.fromDate(new Date(now - 65_000)),
      nextRun: Timestamp.fromDate(new Date(now + 125_000)),
    }),
  );
}

function cellEndingWith(container: HTMLElement, pattern: RegExp): HTMLElement {
  const match = [...container.querySelectorAll("span")].find((el) => pattern.test(el.textContent));
  if (!match) throw new Error(`no cell matching ${pattern}`);
  return match;
}

describe("ScheduledJobsTable run times", () => {
  test("last run reserves the widest elapsed string and never wraps", () => {
    const { container } = render(<ScheduledJobsTable jobs={[scheduledJob()]} dark showNodeBadge={false} />);
    const cell = cellEndingWith(container, /^\d+m \d{2}s ago$/);
    expect(cell.style.minWidth).toBe(`${ELAPSED_MAX_CHARS}ch`);
    expect(cell.className).toContain("whitespace-nowrap");
    expect(cell.className).toContain("tabular-nums");
    expect(cell.className).toContain("text-right");
  });

  test("next run reserves the widest countdown string and never wraps", () => {
    const { container } = render(<ScheduledJobsTable jobs={[scheduledJob()]} dark showNodeBadge={false} />);
    const cell = cellEndingWith(container, /^in \d+m \d{2}s$/);
    expect(cell.style.minWidth).toBe(`${COUNTDOWN_MAX_CHARS}ch`);
    expect(cell.className).toContain("whitespace-nowrap");
    expect(cell.className).toContain("text-right");
  });
});

describe("ChunkProgress", () => {
  test("the done count reserves the width of the total", () => {
    const { container } = render(<ChunkProgress done={7} total={1200} dark />);
    const done = cellEndingWith(container, /^7$/);
    expect(done.style.minWidth).toBe("4ch");
    expect(container.textContent).toBe("7/1200 chunks");
  });
});
