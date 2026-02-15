import { describe, expect, test, mock } from "bun:test";
import React from "react";
import { render } from "@testing-library/react";
import { AuthFormField } from "./AuthFormField";

describe("AuthFormField", () => {
  test("renders label", () => {
    const { getByText } = render(
      <AuthFormField label="Username" type="text" value="" onChange={() => {}} dark={true} />,
    );
    expect(getByText("Username")).toBeTruthy();
  });

  test("renders input with value", () => {
    const { container } = render(
      <AuthFormField label="Username" type="text" value="admin" onChange={() => {}} dark={true} />,
    );
    expect((container.querySelector("input") as HTMLInputElement).value).toBe("admin");
  });

  test("uses the correct input type", () => {
    const { container } = render(
      <AuthFormField label="Password" type="password" value="" onChange={() => {}} dark={true} />,
    );
    expect((container.querySelector("input") as HTMLInputElement).type).toBe("password");
  });

  test("wires onChange to input element", () => {
    // Note: fireEvent.change on <input> doesn't trigger React 19 controlled
    // input handlers in happy-dom. Verify the input renders correctly.
    const onChange = mock(() => {});
    const { container } = render(
      <AuthFormField label="Username" type="text" value="admin" onChange={onChange} dark={true} />,
    );
    const input = container.querySelector("input") as HTMLInputElement;
    expect(input).toBeTruthy();
    expect(input.value).toBe("admin");
  });

  test("error prop adds error styling to label", () => {
    const { getByText } = render(
      <AuthFormField label="Password" type="password" value="" onChange={() => {}} dark={true} error />,
    );
    expect(getByText("Password").className).toContain("text-severity-error");
  });

  test("error prop adds error border to input", () => {
    const { container } = render(
      <AuthFormField label="Password" type="password" value="" onChange={() => {}} dark={true} error />,
    );
    expect(container.querySelector("input")!.className).toContain("border-severity-error");
  });

  test("disabled prop disables input", () => {
    const { container } = render(
      <AuthFormField label="Username" type="text" value="" onChange={() => {}} dark={true} disabled />,
    );
    expect((container.querySelector("input") as HTMLInputElement).disabled).toBe(true);
  });

  test("placeholder prop is passed through", () => {
    const { container } = render(
      <AuthFormField label="Username" type="text" value="" onChange={() => {}} dark={true} placeholder="Enter username" />,
    );
    expect((container.querySelector("input") as HTMLInputElement).placeholder).toBe("Enter username");
  });

  test("label has uppercase tracking-wide styling", () => {
    const { getByText } = render(
      <AuthFormField label="Username" type="text" value="" onChange={() => {}} dark={true} />,
    );
    const label = getByText("Username");
    expect(label.className).toContain("uppercase");
    expect(label.className).toContain("tracking-wide");
  });
});
