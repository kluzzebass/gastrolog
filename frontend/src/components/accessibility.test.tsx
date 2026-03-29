import { describe, test, expect, mock } from "bun:test";
import { render, fireEvent } from "@testing-library/react";
import { Checkbox } from "./settings/Checkbox";
import { ExpandableCard } from "./settings/ExpandableCard";
import { QueryAutocomplete } from "./QueryAutocomplete";
import { Button, DropdownButton } from "./settings/Buttons";
import { ToastProvider, useToast } from "./Toast";

// ── Focus trap mock ──────────────────────────────────────────────────
// focus-trap-react relies on getComputedStyle for visibility checks,
// which happy-dom doesn't support. Provide a passthrough wrapper.

mock.module("focus-trap-react", () => ({
  FocusTrap: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}));

import { Dialog } from "./Dialog";

const noopFn = () => {};

// ── Dialog ───────────────────────────────────────────────────────────

describe("Dialog accessibility", () => {
  test("has role=dialog and aria-modal", () => {
    render(
      <Dialog onClose={noopFn} ariaLabel="Test Dialog" dark={true}>
        <p>content</p>
      </Dialog>,
    );
    const dialog = document.querySelector('[role="dialog"]');
    expect(dialog).toBeTruthy();
    expect(dialog!.getAttribute("aria-modal")).toBe("true");
    expect(dialog!.getAttribute("aria-label")).toBe("Test Dialog");
  });

  test("Escape key calls onClose", () => {
    const onClose = mock(noopFn);
    render(
      <Dialog onClose={onClose} ariaLabel="Test" dark={true}>
        <p>content</p>
      </Dialog>,
    );
    // Dialog listens on globalThis with capture phase
    window.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape" }));
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  test("close button has aria-label", () => {
    render(
      <Dialog onClose={noopFn} ariaLabel="Test" dark={true}>
        <p>content</p>
      </Dialog>,
    );
    const closeBtn = document.querySelector('button[aria-label="Close"]');
    expect(closeBtn).toBeTruthy();
  });

  test("backdrop button has aria-label", () => {
    render(
      <Dialog onClose={noopFn} ariaLabel="Test" dark={true}>
        <p>content</p>
      </Dialog>,
    );
    const backdrop = document.querySelector('button[aria-label="Close dialog"]');
    expect(backdrop).toBeTruthy();
    expect(backdrop!.getAttribute("tabindex")).toBe("-1");
  });

  test("clicking close button calls onClose", () => {
    const onClose = mock(noopFn);
    render(
      <Dialog onClose={onClose} ariaLabel="Test" dark={true}>
        <p>content</p>
      </Dialog>,
    );
    fireEvent.click(document.querySelector('button[aria-label="Close"]')!);
    expect(onClose).toHaveBeenCalledTimes(1);
  });
});

// ── Checkbox keyboard ────────────────────────────────────────────────

describe("Checkbox keyboard interaction", () => {
  test("Enter key toggles checkbox", () => {
    const onChange = mock(noopFn);
    render(
      <Checkbox checked={false} onChange={onChange} dark={true} />,
    );
    const el = document.querySelector('[role="checkbox"]')!;
    fireEvent.keyDown(el, { key: "Enter" });
    expect(onChange).toHaveBeenCalledWith(true);
  });

  test("Space key toggles checkbox", () => {
    const onChange = mock(noopFn);
    render(
      <Checkbox checked={true} onChange={onChange} dark={true} />,
    );
    const el = document.querySelector('[role="checkbox"]')!;
    fireEvent.keyDown(el, { key: " " });
    expect(onChange).toHaveBeenCalledWith(false);
  });

  test("has tabIndex for keyboard focus", () => {
    render(
      <Checkbox checked={false} onChange={noopFn} dark={true} />,
    );
    const el = document.querySelector('[role="checkbox"]')!;
    expect(el.getAttribute("tabindex")).toBe("0");
  });

  test("aria-checked reflects unchecked state", () => {
    render(
      <Checkbox checked={false} onChange={noopFn} dark={true} />,
    );
    expect(document.querySelector('[aria-checked="false"]')).toBeTruthy();
  });
});

// ── ExpandableCard keyboard ──────────────────────────────────────────

describe("ExpandableCard keyboard interaction", () => {
  test("header has role=button and tabIndex", () => {
    render(
      <ExpandableCard id="test" dark={true} onToggle={noopFn}>
        <p>body</p>
      </ExpandableCard>,
    );
    const header = document.querySelector('[role="button"]');
    expect(header).toBeTruthy();
    expect(header!.getAttribute("tabindex")).toBe("0");
  });

  test("Enter key triggers onToggle", () => {
    const onToggle = mock(noopFn);
    render(
      <ExpandableCard id="test" dark={true} onToggle={onToggle}>
        <p>body</p>
      </ExpandableCard>,
    );
    const header = document.querySelector('[role="button"]')!;
    fireEvent.keyDown(header, { key: "Enter" });
    expect(onToggle).toHaveBeenCalledTimes(1);
  });

  test("Space key triggers onToggle", () => {
    const onToggle = mock(noopFn);
    render(
      <ExpandableCard id="test" dark={true} onToggle={onToggle}>
        <p>body</p>
      </ExpandableCard>,
    );
    const header = document.querySelector('[role="button"]')!;
    fireEvent.keyDown(header, { key: " " });
    expect(onToggle).toHaveBeenCalledTimes(1);
  });

  test("aria-expanded false when collapsed", () => {
    render(
      <ExpandableCard id="test" dark={true} expanded={false}>
        <p>body</p>
      </ExpandableCard>,
    );
    expect(document.querySelector('[aria-expanded="false"]')).toBeTruthy();
  });

  test("other keys do not trigger onToggle", () => {
    const onToggle = mock(noopFn);
    render(
      <ExpandableCard id="test" dark={true} onToggle={onToggle}>
        <p>body</p>
      </ExpandableCard>,
    );
    const header = document.querySelector('[role="button"]')!;
    fireEvent.keyDown(header, { key: "Tab" });
    fireEvent.keyDown(header, { key: "a" });
    expect(onToggle).not.toHaveBeenCalled();
  });
});

// ── QueryAutocomplete ARIA ───────────────────────────────────────────

describe("QueryAutocomplete accessibility", () => {
  const suggestions = ["level", "source", "host"];

  test("has role=listbox", () => {
    render(
      <QueryAutocomplete
        suggestions={suggestions}
        selectedIndex={0}
        dark={true}
        onSelect={noopFn}
        onClose={noopFn}
      />,
    );
    expect(document.querySelector('[role="listbox"]')).toBeTruthy();
  });

  test("options have role=option", () => {
    render(
      <QueryAutocomplete
        suggestions={suggestions}
        selectedIndex={0}
        dark={true}
        onSelect={noopFn}
        onClose={noopFn}
      />,
    );
    const options = document.querySelectorAll('[role="option"]');
    expect(options.length).toBe(3);
  });

  test("selected option has aria-selected=true", () => {
    render(
      <QueryAutocomplete
        suggestions={suggestions}
        selectedIndex={1}
        dark={true}
        onSelect={noopFn}
        onClose={noopFn}
      />,
    );
    const options = document.querySelectorAll('[role="option"]');
    expect(options[0]!.getAttribute("aria-selected")).toBe("false");
    expect(options[1]!.getAttribute("aria-selected")).toBe("true");
    expect(options[2]!.getAttribute("aria-selected")).toBe("false");
  });

  test("clicking option calls onSelect with index", () => {
    const onSelect = mock(noopFn);
    render(
      <QueryAutocomplete
        suggestions={suggestions}
        selectedIndex={0}
        dark={true}
        onSelect={onSelect}
        onClose={noopFn}
      />,
    );
    const options = document.querySelectorAll('[role="option"]');
    // QueryAutocomplete uses onMouseDown, not onClick
    fireEvent.mouseDown(options[2]!);
    expect(onSelect).toHaveBeenCalledWith(2);
  });

  test("renders nothing when suggestions is empty", () => {
    render(
      <QueryAutocomplete
        suggestions={[]}
        selectedIndex={0}
        dark={true}
        onSelect={noopFn}
        onClose={noopFn}
      />,
    );
    expect(document.querySelector('[role="listbox"]')).toBeNull();
  });
});

// ── DropdownButton ───────────────────────────────────────────────────

describe("DropdownButton accessibility", () => {
  const items = [
    { value: "a", label: "Alpha" },
    { value: "b", label: "Beta" },
  ];

  test("dropdown menu is hidden by default", () => {
    const { queryByText } = render(
      <DropdownButton label="Actions" items={items} onSelect={noopFn} dark={true} />,
    );
    expect(queryByText("Alpha")).toBeNull();
    expect(queryByText("Beta")).toBeNull();
  });

  test("clicking button opens dropdown menu", () => {
    const { getByText, queryByText } = render(
      <DropdownButton label="Actions" items={items} onSelect={noopFn} dark={true} />,
    );
    fireEvent.click(getByText("Actions"));
    expect(queryByText("Alpha")).toBeTruthy();
    expect(queryByText("Beta")).toBeTruthy();
  });

  test("selecting item calls onSelect and closes menu", () => {
    const onSelect = mock(noopFn);
    const { getByText, queryByText } = render(
      <DropdownButton label="Actions" items={items} onSelect={onSelect} dark={true} />,
    );
    fireEvent.click(getByText("Actions"));
    fireEvent.click(getByText("Alpha"));
    expect(onSelect).toHaveBeenCalledWith("a");
    expect(queryByText("Alpha")).toBeNull();
  });
});

// ── Button disabled state ────────────────────────────────────────────

describe("Button accessibility", () => {
  test("disabled button has disabled attribute", () => {
    const { getByText } = render(
      <Button onClick={noopFn} disabled>Save</Button>,
    );
    const btn = getByText("Save");
    expect(btn.hasAttribute("disabled")).toBe(true);
  });

  test("disabled button does not fire onClick", () => {
    const onClick = mock(noopFn);
    const { getByText } = render(
      <Button onClick={onClick} disabled>Save</Button>,
    );
    fireEvent.click(getByText("Save"));
    expect(onClick).not.toHaveBeenCalled();
  });
});

// ── Toast ARIA ───────────────────────────────────────────────────────

function ToastTrigger({ message, level }: Readonly<{ message: string; level?: "error" | "warn" | "info" }>) {
  const { addToast } = useToast();
  return <button onClick={() => addToast(message, level)}>trigger</button>;
}

describe("Toast accessibility", () => {
  test("toast container has role=status and aria-live", () => {
    const { getByText } = render(
      <ToastProvider dark={true}>
        <ToastTrigger message="Something happened" level="info" />
      </ToastProvider>,
    );
    fireEvent.click(getByText("trigger"));
    const status = document.querySelector('[role="status"]');
    expect(status).toBeTruthy();
    expect(status!.getAttribute("aria-live")).toBe("polite");
  });

  test("toast has dismiss button with aria-label", () => {
    const { getByText } = render(
      <ToastProvider dark={true}>
        <ToastTrigger message="Dismissable toast" level="warn" />
      </ToastProvider>,
    );
    fireEvent.click(getByText("trigger"));
    const dismissBtn = document.querySelector('button[aria-label="Dismiss"]');
    expect(dismissBtn).toBeTruthy();
  });

  test("dismiss button is clickable", () => {
    const { getByText } = render(
      <ToastProvider dark={true}>
        <ToastTrigger message="Gone soon" level="info" />
      </ToastProvider>,
    );
    fireEvent.click(getByText("trigger"));
    const dismissBtn = document.querySelector('button[aria-label="Dismiss"]')!;
    // Verify clicking doesn't throw — actual removal is async via useSyncExternalStore
    fireEvent.click(dismissBtn);
  });
});
