import { describe, test, expect } from "bun:test";
import { useState } from "react";
import { fireEvent } from "@testing-library/react";
import { installMockClients } from "../../../../test/api-mock";
import { createTestQueryClient, settingsWrapper } from "../../../../test/render";

installMockClients();

import { HttpCards } from "./HttpSection";
import { type HTTPLookupDraft, emptyHttpDraft } from "./types";
import type { HTTPLookupEntry } from "../../../api/gen/gastrolog/v1/system_pb";
import { renderWritable } from "../../../testing/renderWritable";

const savedUsers = {
  ...emptyHttpDraft(),
  name: "users",
  urlTemplate: "https://api.example.com/users/{id}",
  allowPrivateDestinations: false,
};

/** HttpCards is controlled by its parent; this harness plays that parent. */
function Harness({ initial }: Readonly<{ initial: HTTPLookupDraft[] }>) {
  const [lookups, setLookups] = useState(initial);
  return (
    <HttpCards
      dark
      addToast={() => {}}
      lookups={lookups}
      savedLookups={[savedUsers as unknown as HTTPLookupEntry]}
      onUpdate={(i, patch) => setLookups((prev) => prev.map((l, j) => (j === i ? { ...l, ...patch } : l)))}
      onDelete={() => {}}
      onRevert={() => {}}
    />
  );
}

function renderCards(initial: HTTPLookupDraft[] = [savedUsers]) {
  const qc = createTestQueryClient();
  const result = renderWritable(<Harness initial={initial} />, { wrapper: settingsWrapper(qc) });
  fireEvent.click(result.getByText("users"));
  return result;
}

describe("HttpCards test gate", () => {
  test("Test is refused while the private-destinations flag is unsaved, and offered again once it matches", () => {
    const { getByText, queryByText } = renderCards();

    expect(getByText("Test").closest("button")!.disabled).toBe(false);
    expect(queryByText(/Save first/)).toBeNull();

    fireEvent.click(getByText("Allow private network destinations"));
    expect(getByText("Test").closest("button")!.disabled).toBe(true);
    expect(getByText("Save first: Test uses the saved private-destinations setting")).toBeTruthy();

    fireEvent.click(getByText("Allow private network destinations"));
    expect(getByText("Test").closest("button")!.disabled).toBe(false);
    expect(queryByText(/Save first/)).toBeNull();
  });

  test("a lookup without a URL template says so beside Test and Save", () => {
    const { getAllByText } = renderCards([{ ...savedUsers, urlTemplate: "" }]);

    expect(getAllByText("URL Template is required")).toHaveLength(2);
  });
});
