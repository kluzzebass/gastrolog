import { createRoute } from "@tanstack/react-router";
import { rootRoute } from "./__root";
import { App } from "../App";

export const searchRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/search",
  validateSearch: (search: Record<string, unknown>) => ({
    q: (search.q as string) || "",
  }),
  component: App,
});
