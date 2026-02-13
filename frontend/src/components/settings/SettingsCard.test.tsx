import { describe, expect, test, mock } from "bun:test";
import React from "react";
import { render, fireEvent } from "@testing-library/react";
import { SettingsCard } from "./SettingsCard";

describe("SettingsCard", () => {
  test("renders id and children when expanded", () => {
    const { getByText } = render(
      <SettingsCard id="my-store" dark={true} expanded={true}>
        <p>card content</p>
      </SettingsCard>,
    );
    expect(getByText("my-store")).toBeTruthy();
    expect(getByText("card content")).toBeTruthy();
  });

  test("renders footer", () => {
    const { getByText } = render(
      <SettingsCard id="my-store" dark={true} expanded={true} footer={<button>Save</button>}>
        <p>content</p>
      </SettingsCard>,
    );
    expect(getByText("Save")).toBeTruthy();
  });

  test("delete button shows initially, then confirm flow", () => {
    const onDelete = mock(() => {});
    const { getByText } = render(
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
    const { getByText } = render(
      <SettingsCard id="my-store" dark={true} expanded={true} onDelete={onDelete}>
        <p>content</p>
      </SettingsCard>,
    );

    fireEvent.click(getByText("Delete"));
    fireEvent.click(getByText("Yes"));
    expect(onDelete).toHaveBeenCalledTimes(1);
  });

  test("cancelling delete goes back to delete button", () => {
    const { getByText, queryByText } = render(
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
    const { getByText } = render(
      <SettingsCard id="my-store" dark={true} expanded={true} onDelete={() => {}} deleteLabel="Remove">
        <p>content</p>
      </SettingsCard>,
    );
    expect(getByText("Remove")).toBeTruthy();
  });

  test("no delete or footer hides action bar", () => {
    const { queryByText } = render(
      <SettingsCard id="my-store" dark={true} expanded={true}>
        <p>content</p>
      </SettingsCard>,
    );
    expect(queryByText("Delete")).toBeNull();
  });
});
