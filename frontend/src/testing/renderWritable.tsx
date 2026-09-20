import { render } from "@testing-library/react";
import type { RenderOptions } from "@testing-library/react";

import { ReadOnlyProvider } from "../hooks/useReadOnly";

/**
 * Render a settings or inspector component as a caller who may change things.
 *
 * Write access is read-only by default (see useReadOnly), so a component
 * mounted bare is inert — which is the right default for the app and the
 * wrong one for a test that exercises editing. Such a test says so by
 * rendering through here, which also keeps "this asserts admin behaviour"
 * visible at the call site.
 *
 * A test that wants the non-admin view renders normally and gets it.
 */
export function renderWritable(
  ui: React.ReactElement,
  options?: RenderOptions,
) {
  return render(<ReadOnlyProvider readOnly={false}>{ui}</ReadOnlyProvider>, options);
}
