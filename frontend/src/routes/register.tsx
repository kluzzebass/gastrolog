import { createRoute } from "@tanstack/react-router";
import { rootRoute } from "./__root";
import { AuthPage } from "../components/auth/AuthPage";

export const registerRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/register",
  component: () => <AuthPage mode="register" />,
});
