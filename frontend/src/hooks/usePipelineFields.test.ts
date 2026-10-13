import { describe, test, expect, beforeEach } from "bun:test";
import { renderHook, waitFor } from "@testing-library/react";
import { installMockClients, m } from "../../test/api-mock";
import { renderHookWithAbandonedRender } from "../../test/abandonedRender";

const mocks = installMockClients();

import { usePipelineFields } from "./usePipelineFields";

type Request = { expression: string; cursor: number; baseFields: string[] };
type Props = { expression: string; cursor: number; baseFields: string[] };

const pipelineFieldsMock = () => m(mocks.queryClient, "getPipelineFields");

function serveFields() {
  const requests: Request[] = [];
  pipelineFieldsMock().mockImplementation((req: Request) => {
    requests.push(req);
    return Promise.resolve({ fields: ["count"], completions: ["stats"] });
  });
  return requests;
}

const debounceElapsed = { timeout: 2000 };

beforeEach(() => {
  pipelineFieldsMock().mockReset();
});

describe("usePipelineFields", () => {
  test("fires once after the debounce with the latest cursor and base fields", async () => {
    const requests = serveFields();
    const { result, rerender } = renderHook(
      (props: Props) => usePipelineFields(props.expression, props.cursor, props.baseFields),
      { initialProps: { expression: "error | ", cursor: 1, baseFields: [] } as Props },
    );
    rerender({ expression: "error | ", cursor: 8, baseFields: ["level"] });

    await waitFor(() => expect(result.current.fields).toEqual(["count"]), debounceElapsed);
    expect(requests).toEqual([{ expression: "error | ", cursor: 8, baseFields: ["level"] }]);
    expect(result.current.completions).toEqual(["stats"]);
  });

  test("sends the committed cursor, not one from a render React threw away", async () => {
    const requests = serveFields();
    const { abandon } = renderHookWithAbandonedRender(
      (props: Props) => usePipelineFields(props.expression, props.cursor, props.baseFields),
      { expression: "error | ", cursor: 1, baseFields: ["level"] },
    );

    await abandon({ expression: "error | ", cursor: 99, baseFields: ["bogus"] });

    await waitFor(() => expect(requests).toHaveLength(1), debounceElapsed);
    expect(requests[0]).toEqual({ expression: "error | ", cursor: 1, baseFields: ["level"] });
  });
});
