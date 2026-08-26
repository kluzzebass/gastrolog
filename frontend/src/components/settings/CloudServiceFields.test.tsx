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
    credentialsConfigured: false,
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

function inputForLabel(container: HTMLElement, labelText: string): HTMLInputElement {
  const label = Array.from(container.querySelectorAll("label")).find(
    (l) => l.textContent === labelText,
  );
  expect(label).toBeTruthy();
  const input = container.querySelector(`#${CSS.escape(label!.htmlFor)}`);
  expect(input).toBeTruthy();
  return input as HTMLInputElement;
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

describe("CloudServiceFields credential state", () => {
  test("a configured service says the empty fields keep the stored credentials", () => {
    const { container } = render(
      <CloudServiceFields
        values={values({ credentialsConfigured: true })}
        onChange={() => {}}
        dark={true}
      />,
    );
    expect(container.textContent).toContain("Credentials are stored");
    expect(container.textContent).toContain("leave the fields below empty to keep them");
  });

  test("credential inputs render empty — no masked stand-in implying a value", () => {
    const { container } = render(
      <CloudServiceFields
        values={values({ credentialsConfigured: true })}
        onChange={() => {}}
        dark={true}
      />,
    );
    for (const label of ["Access Key", "Secret Key"]) {
      const input = inputForLabel(container, label);
      expect(input.value).toBe("");
      expect(input.placeholder).toBe("");
    }
  });

  test("an unconfigured s3 service names the fallback credential chain", () => {
    const { container } = render(
      <CloudServiceFields
        values={values({ credentialsConfigured: false })}
        onChange={() => {}}
        dark={true}
      />,
    );
    expect(container.textContent).toContain("No credentials stored");
    expect(container.textContent).toContain("IAM instance role");
  });

  test("an unconfigured azure service says the connection string is required", () => {
    const { container } = render(
      <CloudServiceFields
        values={values({ provider: "azure", credentialsConfigured: false })}
        onChange={() => {}}
        dark={true}
      />,
    );
    expect(container.textContent).toContain("requires a connection string");
  });

  test("an unconfigured gcs service names Application Default Credentials", () => {
    const { container } = render(
      <CloudServiceFields
        values={values({ provider: "gcs", credentialsConfigured: false })}
        onChange={() => {}}
        dark={true}
      />,
    );
    expect(container.textContent).toContain("Application Default Credentials");
  });
});

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

describe("CloudServiceFields endpoint field per provider", () => {
  function endpointLabel(container: HTMLElement): HTMLLabelElement | undefined {
    return Array.from(container.querySelectorAll("label")).find(
      (l) => l.textContent === "Endpoint",
    );
  }

  test("renders for gcs", () => {
    const { container } = render(
      <CloudServiceFields
        values={values({ provider: "gcs" })}
        onChange={() => {}}
        dark={true}
      />,
    );
    expect(endpointLabel(container)).toBeTruthy();
    expect(container.textContent).toContain("default Google endpoint");
  });

  test("does not render for azure", () => {
    const { container } = render(
      <CloudServiceFields
        values={values({ provider: "azure" })}
        onChange={() => {}}
        dark={true}
      />,
    );
    expect(endpointLabel(container)).toBeUndefined();
  });

  test("gcs scheme-less endpoint shows the inline error state", () => {
    const { container } = render(
      <CloudServiceFields
        values={values({ provider: "gcs", endpoint: "gcs-emulator.local:4443" })}
        onChange={() => {}}
        dark={true}
      />,
    );
    const input = endpointInput(container);
    expect(input.className).toContain("border-severity-error");
    expect(input.title).toContain("no scheme");
    expect(input.title).toContain('"https://gcs-emulator.local:4443"');
  });

  test("gcs https:// endpoint shows no error state", () => {
    const { container } = render(
      <CloudServiceFields
        values={values({ provider: "gcs", endpoint: "https://gcs-emulator.local:4443" })}
        onChange={() => {}}
        dark={true}
      />,
    );
    const input = endpointInput(container);
    expect(input.className).not.toContain("border-severity-error");
    expect(input.title).toBe("");
  });

  test("gcs empty endpoint shows no error state (default Google endpoint)", () => {
    const { container } = render(
      <CloudServiceFields
        values={values({ provider: "gcs", endpoint: "" })}
        onChange={() => {}}
        dark={true}
      />,
    );
    const input = endpointInput(container);
    expect(input.className).not.toContain("border-severity-error");
  });
});
