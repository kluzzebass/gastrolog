import { describe, test, expect, mock } from "bun:test";
import { fireEvent } from "@testing-library/react";
import { installMockClients } from "../../../test/api-mock";
import { createTestQueryClient, settingsWrapper } from "../../../test/render";
import { HelpProvider } from "../../hooks/useHelp";

installMockClients();

import { LogLevelsSettings } from "./LogLevelsSettings";
import { LogLevel } from "../../api/gen/gastrolog/v1/system_pb";
import { renderWritable } from "../../testing/renderWritable";

function renderWithHelp() {
  const qc = createTestQueryClient();
  qc.setQueryData(["system"], { logLevels: { defaultLevel: LogLevel.INFO, rules: [] } });
  qc.setQueryData(["log-components"], []);
  const openHelp = mock((_helpRef?: string) => {});
  const result = renderWritable(
    <HelpProvider onOpen={openHelp}>
      <LogLevelsSettings dark />
    </HelpProvider>,
    { wrapper: settingsWrapper(qc) },
  );
  return { ...result, openHelp };
}

/** The help button rendered in the same header row as a card title. */
function headerHelpButton(title: HTMLElement): HTMLButtonElement {
  const button = title.parentElement!.querySelector('button[aria-label="Help"]');
  expect(button).not.toBeNull();
  return button as HTMLButtonElement;
}

describe("LogLevelsSettings help", () => {
  test("the Rules card header opens the rules section without toggling the card", () => {
    const { getByText, openHelp } = renderWithHelp();
    expect(getByText("Default level")).toBeTruthy();

    fireEvent.click(headerHelpButton(getByText("Rules")));

    expect(openHelp).toHaveBeenCalledWith("log-levels#rules");
    expect(getByText("Default level")).toBeTruthy();
  });

  test("the Components card header opens the components section", () => {
    const { getByText, openHelp } = renderWithHelp();

    fireEvent.click(headerHelpButton(getByText("Components")));

    expect(openHelp).toHaveBeenCalledWith("log-levels#components");
  });
});

function renderWithRules(rules: { pattern: string; level: LogLevel }[]) {
  const qc = createTestQueryClient();
  qc.setQueryData(["system"], { logLevels: { defaultLevel: LogLevel.INFO, rules } });
  qc.setQueryData(["log-components"], []);
  return renderWritable(<LogLevelsSettings dark />, { wrapper: settingsWrapper(qc) });
}

describe("LogLevelsSettings save gates", () => {
  test("an empty new rule disables Save with the reason, until the rule is removed", () => {
    const { getByText, getByLabelText, queryByText } = renderWithRules([]);

    fireEvent.click(getByText("+ Add rule"));
    expect(getByText("Save").closest("button")!.disabled).toBe(true);
    expect(getByText("Enter a pattern or remove the empty rule")).toBeTruthy();

    fireEvent.click(getByLabelText("Remove rule"));
    expect(queryByText("Enter a pattern or remove the empty rule")).toBeNull();
  });

  test("a flagged pattern shows its error under the row and the reason beside Save, until fixed", () => {
    const { getByText, getAllByText, getAllByLabelText, queryByText } = renderWithRules([
      { pattern: "orchestrator", level: LogLevel.DEBUG },
      { pattern: "orchestrator", level: LogLevel.WARN },
    ]);

    expect(getAllByText("Duplicate pattern")).toHaveLength(2);
    expect(getByText("Fix the flagged patterns")).toBeTruthy();
    expect(getByText("Save").closest("button")!.disabled).toBe(true);

    fireEvent.click(getAllByLabelText("Remove rule")[1]!);
    expect(queryByText("Duplicate pattern")).toBeNull();
    expect(queryByText("Fix the flagged patterns")).toBeNull();
    expect(getByText("Save").closest("button")!.disabled).toBe(false);
  });
});
