import { describe, expect, test, mock } from "bun:test";
import React from "react";
import { render, fireEvent } from "@testing-library/react";
import { SettingsCard } from "./SettingsCard";
import { renderWritable } from "../../testing/renderWritable";

describe("SettingsCard", () => {
  test("renders id and children when expanded", () => {
    const { getByText } = renderWritable(
      <SettingsCard id="my-store" dark={true} expanded={true}>
        <p>card content</p>
      </SettingsCard>,
    );
    expect(getByText("my-store")).toBeTruthy();
    expect(getByText("card content")).toBeTruthy();
  });

  test("renders footer", () => {
    const { getByText } = renderWritable(
      <SettingsCard id="my-store" dark={true} expanded={true} footer={<button>Save</button>}>
        <p>content</p>
      </SettingsCard>,
    );
    expect(getByText("Save")).toBeTruthy();
  });

  test("delete button shows initially, then confirm flow", () => {
    const onDelete = mock(() => {});
    const { getByText } = renderWritable(
      <SettingsCard id="my-store" dark={true} expanded={true} onDelete={onDelete}>
        <p>content</p>
      </SettingsCard>,
    );

    const deleteBtn = getByText("Delete");
    expect(deleteBtn).toBeTruthy();
    fireEvent.click(deleteBtn);
    expect(getByText("Confirm?")).toBeTruthy();
    expect(getByText("Yes")).toBeTruthy();
    expect(getByText("No")).toBeTruthy();
    expect(onDelete).not.toHaveBeenCalled();
  });

  test("confirming delete calls onDelete", () => {
    const onDelete = mock(() => {});
    const { getByText } = renderWritable(
      <SettingsCard id="my-store" dark={true} expanded={true} onDelete={onDelete}>
        <p>content</p>
      </SettingsCard>,
    );

    fireEvent.click(getByText("Delete"));
    fireEvent.click(getByText("Yes"));
    expect(onDelete).toHaveBeenCalledTimes(1);
  });

  test("cancelling delete goes back to delete button", () => {
    const { getByText, queryByText } = renderWritable(
      <SettingsCard id="my-store" dark={true} expanded={true} onDelete={() => {}}>
        <p>content</p>
      </SettingsCard>,
    );

    fireEvent.click(getByText("Delete"));
    expect(getByText("Confirm?")).toBeTruthy();

    fireEvent.click(getByText("No"));
    expect(getByText("Delete")).toBeTruthy();
    expect(queryByText("Confirm?")).toBeNull();
  });

  test("custom deleteLabel", () => {
    const { getByText } = renderWritable(
      <SettingsCard id="my-store" dark={true} expanded={true} onDelete={() => {}} deleteLabel="Remove">
        <p>content</p>
      </SettingsCard>,
    );
    expect(getByText("Remove")).toBeTruthy();
  });

  test("no delete or footer hides action bar", () => {
    const { queryByText } = renderWritable(
      <SettingsCard id="my-store" dark={true} expanded={true}>
        <p>content</p>
      </SettingsCard>,
    );
    expect(queryByText("Delete")).toBeNull();
  });
});

describe("SettingsCard delete gates and consequences", () => {
  test("a delete reason disables Delete and is visible beside it", () => {
    const onDelete = mock(() => {});
    const { getByText, queryByText } = renderWritable(
      <SettingsCard id="v" dark={true} expanded={true} onDelete={onDelete} deleteDisabledReason="Destination of route r1">
        <p>content</p>
      </SettingsCard>,
    );
    const deleteBtn = getByText("Delete").closest("button")!;
    expect(deleteBtn.disabled).toBe(true);
    expect(getByText("Destination of route r1")).toBeTruthy();
    fireEvent.click(deleteBtn);
    expect(queryByText("Confirm?")).toBeNull();
    expect(onDelete).not.toHaveBeenCalled();
  });

  test("without a reason Delete is enabled and nothing is shown beside it", () => {
    const { getByText, container } = renderWritable(
      <SettingsCard id="v" dark={true} expanded={true} onDelete={() => {}}>
        <p>content</p>
      </SettingsCard>,
    );
    expect(getByText("Delete").closest("button")!.disabled).toBe(false);
    expect(container.querySelectorAll("li")).toHaveLength(0);
  });

  test("consequences are listed before Yes and gone once confirmed or cancelled", () => {
    const onDelete = mock(() => {});
    const { getByText, queryByText } = renderWritable(
      <SettingsCard
        id="v"
        dark={true}
        expanded={true}
        onDelete={onDelete}
        deleteConsequences={["Removes vault-alpha from node-1, node-2", "Its chunk files stay on disk"]}
      >
        <p>content</p>
      </SettingsCard>,
    );
    expect(queryByText("Removes vault-alpha from node-1, node-2")).toBeNull();
    fireEvent.click(getByText("Delete"));
    expect(getByText("Removes vault-alpha from node-1, node-2")).toBeTruthy();
    expect(getByText("Its chunk files stay on disk")).toBeTruthy();
    expect(onDelete).not.toHaveBeenCalled();

    fireEvent.click(getByText("No"));
    expect(queryByText("Removes vault-alpha from node-1, node-2")).toBeNull();

    fireEvent.click(getByText("Delete"));
    fireEvent.click(getByText("Yes"));
    expect(onDelete).toHaveBeenCalledTimes(1);
    expect(queryByText("Removes vault-alpha from node-1, node-2")).toBeNull();
  });
});
