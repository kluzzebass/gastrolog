import { describe, test, expect } from "bun:test";
import { isIngesterParamsValid, missingIngesterParamsReason } from "./validation";

describe("missingIngesterParamsReason", () => {
  test("names the one field a type cannot do without", () => {
    expect(missingIngesterParamsReason("tail", {})).toBe("File Patterns is required");
    expect(missingIngesterParamsReason("relp", {})).toBe("Listen Address is required");
  });

  test("joins several required fields with 'and'", () => {
    expect(missingIngesterParamsReason("kafka", {})).toBe("Brokers and Topic are required");
    expect(missingIngesterParamsReason("kafka", { brokers: "k:9092" })).toBe("Topic is required");
  });

  test("names the alternatives of an either-or requirement", () => {
    expect(missingIngesterParamsReason("syslog", {})).toBe("UDP Address or TCP Address is required");
    expect(missingIngesterParamsReason("otlp", { grpc_addr: ":4317" })).toBeUndefined();
  });

  test("is silent for a type with nothing required, and once every field is set", () => {
    expect(missingIngesterParamsReason("chatterbox", {})).toBeUndefined();
    expect(missingIngesterParamsReason("mqtt", { broker: "tcp://b", topics: "t" })).toBeUndefined();
  });

  test("isIngesterParamsValid is the absence of a reason", () => {
    expect(isIngesterParamsValid("tail", {})).toBe(false);
    expect(isIngesterParamsValid("tail", { paths: '["/var/log/*.log"]' })).toBe(true);
  });
});
