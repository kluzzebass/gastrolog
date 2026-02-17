import { createRoute } from "@tanstack/react-router";
import { rootRoute } from "./__root";
import { SearchView } from "../components/SearchView";

export const followRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/follow",
  validateSearch: (search: Record<string, unknown>) => ({
    q: (search.q as string) || "",
  }),
  component: SearchView,
});
