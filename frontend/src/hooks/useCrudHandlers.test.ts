import { describe, expect, test, mock } from "bun:test";
import React from "react";
import { renderHook, act } from "@testing-library/react";
import { useCrudHandlers } from "./useCrudHandlers";
import { ToastProvider } from "../components/Toast";

const wrapper = ({ children }: { children: React.ReactNode }) =>
  React.createElement(ToastProvider, null, children);

function makeMutation() {
  return {
    mutateAsync: mock(() => Promise.resolve()),
    isPending: false,
  };
}

describe("useCrudHandlers", () => {
  test("handleSave calls mutation with transformed args", async () => {
    const mutation = makeMutation();
    const deleteMutation = makeMutation();

    const { result } = renderHook(
      () =>
        useCrudHandlers({
          mutation,
          deleteMutation,
          label: "Item",
          onSaveTransform: (id, edit: { value: string }) => ({
            id,
            value: edit.value,
          }),
        }),
      { wrapper },
    );

    await act(async () => {
      await result.current.handleSave("item1", { value: "hello" });
    });

    expect(mutation.mutateAsync).toHaveBeenCalledWith({
      id: "item1",
      value: "hello",
    });
  });

  test("handleSave calls clearEdit on success", async () => {
    const mutation = makeMutation();
    const deleteMutation = makeMutation();
    const clearEdit = mock(() => {});

    const { result } = renderHook(
      () =>
        useCrudHandlers({
          mutation,
          deleteMutation,
          label: "Item",
          onSaveTransform: (id, edit: { value: string }) => ({ id, ...edit }),
          clearEdit,
        }),
      { wrapper },
    );

    await act(async () => {
      await result.current.handleSave("item1", { value: "hello" });
    });

    expect(clearEdit).toHaveBeenCalledWith("item1");
  });

  test("handleSave catches errors from onSaveTransform", async () => {
    const mutation = makeMutation();
    const deleteMutation = makeMutation();

    const { result } = renderHook(
      () =>
        useCrudHandlers({
          mutation,
          deleteMutation,
          label: "Item",
          onSaveTransform: () => {
            throw new Error("validation failed");
          },
        }),
      { wrapper },
    );

    // Should not throw
    await act(async () => {
      await result.current.handleSave("item1", {});
    });

    // Mutation should NOT have been called
    expect(mutation.mutateAsync).not.toHaveBeenCalled();
  });

  test("handleSave catches mutation errors", async () => {
    const mutation = {
      mutateAsync: mock(() => Promise.reject(new Error("network error"))),
      isPending: false,
    };
    const deleteMutation = makeMutation();

    const { result } = renderHook(
      () =>
        useCrudHandlers({
          mutation,
          deleteMutation,
          label: "Item",
          onSaveTransform: (_id, edit) => edit,
        }),
      { wrapper },
    );

    // Should not throw
    await act(async () => {
      await result.current.handleSave("item1", {});
    });
  });

  test("handleDelete calls deleteMutation", async () => {
    const mutation = makeMutation();
    const deleteMutation = makeMutation();

    const { result } = renderHook(
      () =>
        useCrudHandlers({
          mutation,
          deleteMutation,
          label: "Item",
          onSaveTransform: (_id, edit) => edit,
        }),
      { wrapper },
    );

    await act(async () => {
      await result.current.handleDelete("item1");
    });

    expect(deleteMutation.mutateAsync).toHaveBeenCalledWith("item1");
  });

  test("handleDelete respects onDeleteCheck", async () => {
    const mutation = makeMutation();
    const deleteMutation = makeMutation();

    const { result } = renderHook(
      () =>
        useCrudHandlers({
          mutation,
          deleteMutation,
          label: "Item",
          onSaveTransform: (_id, edit) => edit,
          onDeleteCheck: (id) =>
            id === "protected" ? "Cannot delete this" : null,
        }),
      { wrapper },
    );

    // Protected item — should not call deleteMutation
    await act(async () => {
      await result.current.handleDelete("protected");
    });
    expect(deleteMutation.mutateAsync).not.toHaveBeenCalled();

    // Non-protected item — should call deleteMutation
    await act(async () => {
      await result.current.handleDelete("other");
    });
    expect(deleteMutation.mutateAsync).toHaveBeenCalledWith("other");
  });

  test("handleDelete calls onDeleteSuccess instead of default toast", async () => {
    const mutation = makeMutation();
    const deleteMutation = makeMutation();
    const onDeleteSuccess = mock(() => {});

    const { result } = renderHook(
      () =>
        useCrudHandlers({
          mutation,
          deleteMutation,
          label: "Item",
          onSaveTransform: (_id, edit) => edit,
          onDeleteSuccess,
        }),
      { wrapper },
    );

    await act(async () => {
      await result.current.handleDelete("item1");
    });

    expect(onDeleteSuccess).toHaveBeenCalledWith("item1");
  });

  test("handleDelete catches errors", async () => {
    const mutation = makeMutation();
    const deleteMutation = {
      mutateAsync: mock(() => Promise.reject(new Error("delete failed"))),
      isPending: false,
    };

    const { result } = renderHook(
      () =>
        useCrudHandlers({
          mutation,
          deleteMutation,
          label: "Item",
          onSaveTransform: (_id, edit) => edit,
        }),
      { wrapper },
    );

    // Should not throw
    await act(async () => {
      await result.current.handleDelete("item1");
    });
  });
});
