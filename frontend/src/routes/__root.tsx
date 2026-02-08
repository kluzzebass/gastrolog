import { createRootRoute, Outlet, redirect } from "@tanstack/react-router";
import { getToken } from "../api/client";

const AUTH_PATHS = ["/login", "/register"];

export const rootRoute = createRootRoute({
  beforeLoad: ({ location }) => {
    if (AUTH_PATHS.includes(location.pathname)) return;
    if (!getToken()) {
      throw redirect({ to: "/login" });
    }
  },
  component: () => <Outlet />,
});
