import { Suspense, startTransition, use, useEffect, useState, type ReactNode } from "react";
import { act, render } from "@testing-library/react";

const never = new Promise<never>(() => {});

/**
 * `act` for a test with an abandoned render pending: React retries the
 * suspended render inside every act, and an act it suspends in must be awaited.
 */
export function actAwaited(fn: () => void) {
  return act(() => {
    fn();
    return Promise.resolve();
  });
}

function Suspender({ suspend }: Readonly<{ suspend: boolean }>) {
  if (suspend) use(never);
  return null;
}

/**
 * Renders `ui(props)` and lets a test start a render that React throws away.
 *
 * `abandon(props)` renders the tree with `props` inside a transition that
 * suspends forever, so React keeps showing the committed tree and never
 * commits the new one. Anything the abandoned render did besides computing
 * its output — writing a ref, calling a parent's callback — is a leak the
 * committed tree can observe.
 */
export function renderWithAbandonedRender<P>(ui: (props: P) => ReactNode, initialProps: P) {
  let setState: ((next: { props: P; suspend: boolean }) => void) | undefined;

  function Root() {
    const [state, set] = useState({ props: initialProps, suspend: false });
    useEffect(() => {
      setState = set;
    }, []);
    return (
      <Suspense fallback={null}>
        {ui(state.props)}
        <Suspender suspend={state.suspend} />
      </Suspense>
    );
  }

  const rendered = render(<Root />);
  return {
    ...rendered,
    rerender: (props: P) => {
      act(() => setState?.({ props, suspend: false }));
    },
    abandon: async (props: P, alongside?: () => void) => {
      await actAwaited(() => {
        startTransition(() => {
          alongside?.();
          setState?.({ props, suspend: true });
        });
      });
    },
  };
}

/**
 * Hook flavour of {@link renderWithAbandonedRender}: `result.current` is the
 * hook's value from the last committed render.
 */
export function renderHookWithAbandonedRender<P, R>(useHook: (props: P) => R, initialProps?: P) {
  const result = { current: undefined as R };

  function HookHost({ hookProps }: Readonly<{ hookProps: P }>) {
    const value = useHook(hookProps);
    useEffect(() => {
      result.current = value;
    });
    return null;
  }

  const view = renderWithAbandonedRender((hookProps: P) => <HookHost hookProps={hookProps} />, initialProps as P);
  return { result, rerender: view.rerender, abandon: view.abandon, unmount: view.unmount };
}
