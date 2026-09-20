import { describe, expect, test } from "bun:test";
import React from "react";
import { render } from "@testing-library/react";

import { ReadOnlyProvider } from "../../hooks/useReadOnly";
import { renderWritable } from "../../testing/renderWritable";
import { Checkbox } from "./Checkbox";
import { FormField, SelectInput, TextArea, TextInput } from "./FormField";
import { SettingsCard } from "./SettingsCard";
import { AddFormCard } from "./AddFormCard";

// Every config write is admin-only, so a caller who cannot write must not be
// shown a form that invites one. What they must still be able to do is read:
// values stay on screen and stay selectable, because reading them is why they
// opened the page.

function readOnly(ui: React.ReactElement) {
  return render(<ReadOnlyProvider readOnly={true}>{ui}</ReadOnlyProvider>);
}

describe("read-only settings: values remain readable", () => {
  test("a text input shows its value and refuses edits", () => {
    const { container } = readOnly(
      <FormField label="Endpoint" dark={true}>
        <TextInput value="https://minio.example:9000" onChange={() => {}} dark={true} />
      </FormField>,
    );
    const input = container.querySelector("input");
    expect(input).not.toBeNull();
    // readOnly, not disabled: a disabled input cannot be selected or copied,
    // and the value is the thing the caller came for.
    expect(input?.readOnly).toBe(true);
    expect(input?.disabled).toBe(false);
    expect(input?.value).toBe("https://minio.example:9000");
  });

  test("a textarea refuses edits but keeps its contents", () => {
    const { container } = readOnly(
      <TextArea value="line one" onChange={() => {}} dark={true} />,
    );
    const area = container.querySelector("textarea");
    expect(area?.readOnly).toBe(true);
    expect(area?.value).toBe("line one");
  });

  test("a select cannot be changed", () => {
    // HTML has no readOnly for select, so disabled is the only way to stop it.
    const { container } = readOnly(
      <SelectInput
        value="s3"
        onChange={() => {}}
        options={[{ value: "s3", label: "S3" }]}
        dark={true}
      />,
    );
    expect(container.querySelector("select")?.disabled).toBe(true);
  });

  test("a checkbox shows its state and cannot be toggled", () => {
    let toggled = false;
    const { container } = readOnly(
      <Checkbox checked={true} onChange={() => { toggled = true; }} dark={true} />,
    );
    (container.firstElementChild as HTMLElement | null)?.click();
    expect(toggled).toBe(false);
  });
});

describe("read-only settings: nothing offers a write", () => {
  test("the card's save footer is absent", () => {
    const { queryByText } = readOnly(
      <SettingsCard id="archive" dark={true} expanded footer={<button>Save</button>}>
        <span>body</span>
      </SettingsCard>,
    );
    expect(queryByText("Save")).toBeNull();
  });

  test("the card's delete control is absent", () => {
    const { queryByText } = readOnly(
      <SettingsCard id="archive" dark={true} expanded onDelete={() => {}}>
        <span>body</span>
      </SettingsCard>,
    );
    expect(queryByText("Delete")).toBeNull();
  });

  test("the add form is not rendered at all", () => {
    const { container } = readOnly(
      <AddFormCard dark={true} onCancel={() => {}} onCreate={() => {}} isPending={false}>
        <span>new entity</span>
      </AddFormCard>,
    );
    expect(container.textContent).toBe("");
  });
});

describe("write access restores every control", () => {
  test("the same card shows its footer and delete", () => {
    const { queryByText } = renderWritable(
      <SettingsCard id="archive" dark={true} expanded onDelete={() => {}} footer={<button>Save</button>}>
        <span>body</span>
      </SettingsCard>,
    );
    expect(queryByText("Save")).not.toBeNull();
    expect(queryByText("Delete")).not.toBeNull();
  });

  test("the same input is editable", () => {
    const { container } = renderWritable(
      <TextInput value="https://minio.example:9000" onChange={() => {}} dark={true} />,
    );
    expect(container.querySelector("input")?.readOnly).toBe(false);
  });

  test("the add form renders", () => {
    const { container } = renderWritable(
      <AddFormCard dark={true} onCancel={() => {}} onCreate={() => {}} isPending={false}>
        <span>new entity</span>
      </AddFormCard>,
    );
    expect(container.textContent).toContain("new entity");
  });
});

// The default matters: a surface that forgets to declare write access should
// be inert rather than offering writes the server will refuse.
describe("the default is read-only", () => {
  test("a bare input is not editable", () => {
    const { container } = render(
      <TextInput value="x" onChange={() => {}} dark={true} />,
    );
    expect(container.querySelector("input")?.readOnly).toBe(true);
  });
});
