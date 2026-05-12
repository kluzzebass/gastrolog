import { describe, expect, test } from "bun:test";
import React from "react";
import { render } from "@testing-library/react";
import { EnvironmentBanner } from "./EnvironmentBanner";

describe("EnvironmentBanner", () => {
  test("renders the provided label", () => {
    const { getByText } = render(
      <EnvironmentBanner label="Kubernetes" color="red" dark={true} />,
    );
    expect(getByText("Kubernetes")).toBeTruthy();
  });

  test("applies the color as an inline style", () => {
    const { container } = render(
      <EnvironmentBanner label="Production" color="#c4302b" dark={true} />,
    );
    const el = container.firstChild as HTMLElement;
    // happy-dom / jsdom serialize differently (hex vs rgb()); either is fine
    // as long as the operator's input reached the inline style attribute.
    expect(el.style.color.length).toBeGreaterThan(0);
    expect(el.getAttribute("style") ?? "").toContain("#c4302b");
  });

  test("empty color leaves the inline style unset so palette default wins", () => {
    const { container } = render(
      <EnvironmentBanner label="Development" color="" dark={true} />,
    );
    const el = container.firstChild as HTMLElement;
    expect(el.style.color).toBe("");
  });

  test("invalid color string does not throw and falls back to no inline style", () => {
    // Browsers silently ignore unknown CSS color values when applied via the
    // CSSStyleDeclaration setter, leaving style.color empty. The component
    // must not crash on operator-typo inputs.
    const { container } = render(
      <EnvironmentBanner label="Staging" color="not-a-real-color" dark={true} />,
    );
    const el = container.firstChild as HTMLElement;
    // The style attribute is set on the DOM but the browser dropped the
    // invalid value, so style.color reads empty.
    expect(el.style.color).toBe("");
  });

  test("light and dark modes both render the label", () => {
    const dark = render(
      <EnvironmentBanner label="Kubernetes-Dark" color="red" dark={true} />,
    );
    expect(dark.getByText("Kubernetes-Dark")).toBeTruthy();
    dark.unmount();
    const light = render(
      <EnvironmentBanner label="Kubernetes-Light" color="red" dark={false} />,
    );
    expect(light.getByText("Kubernetes-Light")).toBeTruthy();
  });

  test("very long labels render without crashing (no internal truncation)", () => {
    const longLabel = "Production-EU-Frankfurt-Cluster-7";
    const { getByText } = render(
      <EnvironmentBanner label={longLabel} color="orange" dark={true} />,
    );
    expect(getByText(longLabel)).toBeTruthy();
  });

  test("title attribute carries the label for hover discoverability", () => {
    const { container } = render(
      <EnvironmentBanner label="Staging" color="" dark={true} />,
    );
    const el = container.firstChild as HTMLElement;
    expect(el.title).toBe("Environment: Staging");
  });
});
