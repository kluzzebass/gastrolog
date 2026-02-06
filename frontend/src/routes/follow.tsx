import { createRoute } from "@tanstack/react-router";
import { rootRoute } from "./__root";
import { App } from "../App";

export const followRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/follow",
  validateSearch: (search: Record<string, unknown>) => ({
    q: (search.q as string) || "",
  }),
  component: App,
});
