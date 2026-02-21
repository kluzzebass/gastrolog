import { lazy } from "react";
import { createRoute } from "@tanstack/react-router";
import { rootRoute } from "./__root";

const AuthPage = lazy(() => import("../components/auth/AuthPage").then((m) => ({ default: m.AuthPage })));

export const loginRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/login",
  component: () => <AuthPage mode="login" />,
});
