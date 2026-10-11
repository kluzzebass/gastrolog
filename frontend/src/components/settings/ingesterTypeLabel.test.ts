import { describe, test, expect } from "bun:test";
import { ingesterHelpRef, ingesterTypeLabel } from "./ingesterTypeLabel";
import { findTopic } from "../../help/topics";

describe("ingesterTypeLabel", () => {
  test("a type's label is its help topic's title", () => {
    expect(ingesterTypeLabel("fluentfwd")).toBe("Fluent Forward");
    expect(ingesterTypeLabel("otlp")).toBe("OTLP");
    expect(ingesterTypeLabel("relp")).toBe("RELP");
    expect(ingesterTypeLabel("http")).toBe("HTTP (Loki)");
    expect(ingesterTypeLabel("syslog")).toBe("Syslog");
  });

  test("every configurable type has a help topic, so no label falls back to the raw type", () => {
    for (const type of ["chatterbox", "docker", "fluentfwd", "http", "kafka", "mqtt", "metrics", "otlp", "relp", "scatterbox", "self", "syslog", "tail"]) {
      expect(findTopic(ingesterHelpRef(type))?.title).toBe(ingesterTypeLabel(type));
      expect(ingesterTypeLabel(type)).not.toBe(type);
    }
  });

  test("a type without a topic keeps its raw name rather than vanishing", () => {
    expect(ingesterTypeLabel("carrier-pigeon")).toBe("carrier-pigeon");
  });
});
