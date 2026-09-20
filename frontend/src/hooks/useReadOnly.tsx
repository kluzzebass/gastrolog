import { createContext, useContext } from "react";

/**
 * Whether this session may change anything it is looking at.
 *
 * Every config write on the backend is admin-only, so a non-admin who is
 * shown an editable form is being invited to do something the server will
 * refuse. The interface has to say what the caller can actually do, and the
 * answer is the same on every surface, so it travels by context rather than
 * through the props of every card and field between the dialog and the input.
 *
 * Read-only means read-only, not disabled: values stay selectable and
 * copyable, because reading them is exactly what the caller is here for.
 * Controls that exist only to modify something are not rendered at all — a
 * greyed-out Save still tells someone to try.
 *
 * The default is read-only. A surface that can write says so by wrapping
 * itself in the provider, so a new panel that forgets is inert rather than
 * offering writes that fail.
 */
const ReadOnlyContext = createContext<boolean>(true);

export function ReadOnlyProvider({
  children,
  readOnly,
}: Readonly<{
  children: React.ReactNode;
  readOnly: boolean;
}>) {
  return (
    <ReadOnlyContext.Provider value={readOnly}>
      {children}
    </ReadOnlyContext.Provider>
  );
}

/** True when the surrounding surface must not offer any modification. */
export function useReadOnly(): boolean {
  return useContext(ReadOnlyContext);
}
