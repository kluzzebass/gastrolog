import { describe, expect, test } from "bun:test";
import React from "react";
import { render } from "@testing-library/react";
import { CloudServiceFields, type CloudServiceFieldValues } from "./CloudServiceFields";

function values(patch: Partial<CloudServiceFieldValues>): CloudServiceFieldValues {
  return {
    provider: "s3",
    bucket: "logs",
    region: "us-east-1",
    endpoint: "",
    accessKey: "",
    secretKey: "",
    container: "",
    connectionString: "",
    credentialsJson: "",
    archivalMode: "none",
    transitions: [],
    restoreSpeed: "",
    restoreDays: 7,
    suspectGraceDays: 7,
    reconcileSchedule: "0 3 * * *",
    ...patch,
  };
}

function endpointInput(container: HTMLElement): HTMLInputElement {
  // The endpoint field is the only S3 input whose value we control here;
  // find it by its current value via the label association.
  const label = Array.from(container.querySelectorAll("label")).find(
    (l) => l.textContent === "Endpoint",
  );
  expect(label).toBeTruthy();
  const input = container.querySelector(`#${CSS.escape(label!.htmlFor)}`);
  expect(input).toBeTruthy();
  return input as HTMLInputElement;
}

describe("CloudServiceFields endpoint validation", () => {
  test("scheme-less endpoint shows the inline error state", () => {
    const { container } = render(
      <CloudServiceFields
        values={values({ endpoint: "minio.local:9000" })}
        onChange={() => {}}
        dark={true}
      />,
    );
    const input = endpointInput(container);
    expect(input.className).toContain("border-severity-error");
    expect(input.title).toContain("no scheme");
    expect(input.title).toContain('"https://minio.local:9000"');
  });

  test("https:// endpoint shows no error state", () => {
    const { container } = render(
      <CloudServiceFields
        values={values({ endpoint: "https://minio.local:9000" })}
        onChange={() => {}}
        dark={true}
      />,
    );
    const input = endpointInput(container);
    expect(input.className).not.toContain("border-severity-error");
    expect(input.title).toBe("");
  });

  test("empty endpoint shows no error state (optional for AWS S3)", () => {
    const { container } = render(
      <CloudServiceFields
        values={values({ endpoint: "" })}
        onChange={() => {}}
        dark={true}
      />,
    );
    const input = endpointInput(container);
    expect(input.className).not.toContain("border-severity-error");
  });
});
