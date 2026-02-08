import { createRoute } from "@tanstack/react-router";
import { rootRoute } from "./__root";
import { AuthPage } from "../components/auth/AuthPage";

export const loginRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/login",
  component: () => <AuthPage mode="login" />,
});
