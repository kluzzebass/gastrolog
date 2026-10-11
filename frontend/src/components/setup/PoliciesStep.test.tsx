import { describe, test, expect } from "bun:test";
import { render } from "@testing-library/react";
import { RotationPolicyStep, RetentionPolicyStep } from "./PoliciesStep";
import { ReviewStep } from "./ReviewStep";

const rotation = { name: "", maxAge: "1h", maxSize: "256MB", maxRecords: "", cron: "" };
const retention = { name: "", maxChunks: "", maxAge: "", maxSize: "50GB" };

// The size bound is "Max Size" everywhere the operator meets it: the policy
// settings, the wizard steps and the wizard review, matching the proto field.
describe("setup wizard policy steps", () => {
  test("the rotation step's size bound is labelled Max Size", () => {
    const { getByText, queryByText } = render(
      <RotationPolicyStep dark rotation={rotation} onRotationChange={() => {}} />,
    );
    expect(getByText("Max Size")).toBeTruthy();
    expect(queryByText("Max Bytes")).toBeNull();
  });

  test("the retention step's size bound is labelled Max Size", () => {
    const { getByText, queryByText } = render(
      <RetentionPolicyStep dark retention={retention} onRetentionChange={() => {}} />,
    );
    expect(getByText("Max Size")).toBeTruthy();
    expect(queryByText("Max Bytes")).toBeNull();
  });

  test("the review step reports both size bounds as Max Size", () => {
    const { getAllByText, queryByText, getByText } = render(
      <ReviewStep
        dark
        vault={{ name: "", type: "file", storageClass: "", path: "", memoryBudget: "", nodeId: "" } as never}
        rotation={rotation}
        retention={retention}
        ingester={{ type: "", name: "", params: {} } as never}
      />,
    );
    expect(getAllByText("Max Size").length).toBe(2);
    expect(getByText("256MB")).toBeTruthy();
    expect(getByText("50GB")).toBeTruthy();
    expect(queryByText("Max Bytes")).toBeNull();
  });
});
