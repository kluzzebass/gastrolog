import { createRootRoute, redirect } from "@tanstack/react-router";
import { getToken } from "../api/client";
import { App } from "../App";

const AUTH_PATHS = new Set(["/login", "/register"]);

export const rootRoute = createRootRoute({
  beforeLoad: ({ location }) => {
    if (AUTH_PATHS.has(location.pathname)) return;
    if (!getToken()) {
      // eslint-disable-next-line @typescript-eslint/only-throw-error -- TanStack Router control flow
      throw redirect({ to: "/login" });
    }
  },
  component: App,
});
