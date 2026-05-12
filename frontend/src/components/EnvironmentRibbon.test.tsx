import { describe, expect, test } from "bun:test";
import React from "react";
import { render } from "@testing-library/react";
import {
  EnvironmentRibbon,
  pickContrastingTextColor,
} from "./EnvironmentRibbon";

describe("EnvironmentRibbon", () => {
  test("renders the provided label", () => {
    const { getByText } = render(
      <EnvironmentRibbon label="Kubernetes" color="red" />,
    );
    expect(getByText("Kubernetes")).toBeTruthy();
  });

  test("renders nothing when label is empty", () => {
    const { container } = render(<EnvironmentRibbon label="" color="red" />);
    expect(container.firstChild).toBeNull();
  });

  test("applies the background color as an inline style", () => {
    const { getByText } = render(
      <EnvironmentRibbon label="Production" color="#c4302b" />,
    );
    const stripe = getByText("Production") as HTMLElement;
    // happy-dom / jsdom may serialize hex vs rgb() differently; both forms
    // are valid as long as the operator's input reached the style attribute.
    expect(stripe.getAttribute("style") ?? "").toContain("#c4302b");
  });

  test("auto-picks white text on a dark background (red)", () => {
    const { getByText } = render(
      <EnvironmentRibbon label="K8s" color="red" />,
    );
    const stripe = getByText("K8s") as HTMLElement;
    expect(stripe.style.color).toBe("white");
  });

  test("auto-picks black text on a light background (yellow)", () => {
    const { getByText } = render(
      <EnvironmentRibbon label="Staging" color="yellow" />,
    );
    const stripe = getByText("Staging") as HTMLElement;
    expect(stripe.style.color).toBe("black");
  });

  test("invalid color does not crash and falls back to white text", () => {
    const { getByText } = render(
      <EnvironmentRibbon label="Wat" color="not-a-real-color" />,
    );
    const stripe = getByText("Wat") as HTMLElement;
    expect(stripe.style.color).toBe("white");
  });

  test("very long labels still render", () => {
    const longLabel = "Production-EU-Frankfurt-Cluster-7";
    const { getByText } = render(
      <EnvironmentRibbon label={longLabel} color="orange" />,
    );
    expect(getByText(longLabel)).toBeTruthy();
  });

  test("title attribute carries the label for hover discoverability", () => {
    const { getByText } = render(
      <EnvironmentRibbon label="Staging" color="lime" />,
    );
    const stripe = getByText("Staging") as HTMLElement;
    expect(stripe.title).toBe("Environment: Staging");
  });
});

describe("pickContrastingTextColor", () => {
  test("returns white on empty input", () => {
    expect(pickContrastingTextColor("")).toBe("white");
  });

  test("returns white on a dark named color", () => {
    expect(pickContrastingTextColor("red")).toBe("white");
    expect(pickContrastingTextColor("navy")).toBe("white");
    expect(pickContrastingTextColor("darkblue")).toBe("white");
  });

  test("returns black on a light named color", () => {
    expect(pickContrastingTextColor("yellow")).toBe("black");
    expect(pickContrastingTextColor("white")).toBe("black");
    expect(pickContrastingTextColor("lightyellow")).toBe("black");
  });

  test("returns white on an unparseable string (no resolved channels)", () => {
    // The browser's color parser drops invalid values, leaving the probe's
    // computed color at the default. We treat that as the safe fallback.
    expect(pickContrastingTextColor("totally-bogus")).toBe("white");
  });

  test("handles hex codes", () => {
    expect(pickContrastingTextColor("#000000")).toBe("white");
    expect(pickContrastingTextColor("#ffffff")).toBe("black");
  });
});
