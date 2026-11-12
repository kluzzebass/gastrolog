import { describe, test, expect } from "bun:test";
import { render, fireEvent } from "@testing-library/react";
import { useState } from "react";

function ControlledInput() {
  const [value, setValue] = useState("");
  return (
    <>
      <input placeholder="typed" value={value} onChange={(e) => setValue(e.target.value)} />
      <output>{value}</output>
    </>
  );
}

// Every test that types into a form depends on this: if react-dom loads
// before the test DOM is registered, text inputs silently never call onChange.
describe("test environment", () => {
  test("typing into a controlled text input reaches React state", () => {
    const ui = render(<ControlledInput />);
    fireEvent.change(ui.getByPlaceholderText("typed"), { target: { value: "abc" } });
    expect(ui.container.querySelector("output")!.textContent).toBe("abc");
  });
});
