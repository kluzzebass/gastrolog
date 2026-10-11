import { describe, test, expect, type Mock } from "bun:test";
import { fireEvent } from "@testing-library/react";
import { installMockClients } from "../../../test/api-mock";
import { createTestQueryClient, settingsWrapper } from "../../../test/render";

installMockClients();

import { getToken } from "../../api/client";
import { UsersSettings } from "./UsersSettings";
import { renderWritable } from "../../testing/renderWritable";

function testId(n: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

/** A token whose payload names the signed-in user; the signature is not checked client-side. */
function tokenFor(username: string): string {
  return `h.${btoa(JSON.stringify({ sub: username, role: "admin" }))}.s`;
}

function renderUsers() {
  (getToken as unknown as Mock<() => string | null>).mockReturnValue(tokenFor("admin"));
  const qc = createTestQueryClient();
  qc.setQueryData(["users"], [
    { id: testId(1), username: "admin", role: "admin", createdAt: BigInt(0) },
    { id: testId(2), username: "bob", role: "user", createdAt: BigInt(0) },
  ]);
  qc.setQueryData(["settings"], { nodeId: testId(40), minPasswordLength: 8 });
  return renderWritable(<UsersSettings dark />, { wrapper: settingsWrapper(qc) });
}

describe("UsersSettings own role", () => {
  test("the signed-in user's role select is disabled and says why", () => {
    const { getAllByText, getByText, getByLabelText } = renderUsers();

    // The card title and the role badge both read "admin"; the title comes first.
    fireEvent.click(getAllByText("admin")[0]!);
    expect((getByLabelText("Role") as HTMLSelectElement).disabled).toBe(true);
    expect(getByText("Your own role is locked here — another admin changes it.")).toBeTruthy();
  });

  test("another user's role select is enabled with nothing beside it", () => {
    const { getByText, getByLabelText, queryByText } = renderUsers();

    fireEvent.click(getByText("bob"));
    expect((getByLabelText("Role") as HTMLSelectElement).disabled).toBe(false);
    expect(queryByText(/Your own role is locked/)).toBeNull();
  });
});
